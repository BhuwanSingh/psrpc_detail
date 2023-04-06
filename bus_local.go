// This is a Go package that implements a NATS message bus for publishing and subscribing to messages
// using protobuf.
// The below code defines a local message bus implementation in Go that uses channels to send messages.
package psrpc

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"
)

// The localMessageBus is a MessageBus implementation that uses channels to send messages and contains
// a map of subscribers and queues.
// @property  - The code defines a struct called `localMessageBus` which is an implementation of a
// message bus using channels to send messages.
// @property subs - subs is a map that stores the list of subscribers for each channel in the
// localMessageBus. The key of the map is the channel name and the value is a pointer to a
// localSubList, which is a list of subscribers to that channel.
// @property queues - `queues` is a map of channel name to a list of queue subscribers. This means that
// it stores the subscribers who want to receive messages from a channel in a queue fashion, where
// messages are delivered to each subscriber in the order they were received.
type localMessageBus struct { // localMessageBus is a MessageBus implementation that uses channels to send messages.
	sync.RWMutex                          // RWMutex is used to lock the localMessageBus while holding the localSubList lock is allowed
	subs         map[string]*localSubList // subs is a map of channel name to a list of subscribers
	queues       map[string]*localSubList // queues is a map of channel name to a list of queue subscribers
	// localSubList is a list of subscribers to a channel.
}

// The function creates a new local message bus with initialized subscriber and queue maps.
func NewLocalMessageBus() MessageBus { // NewLocalMessageBus creates a new localMessageBus.
	return &localMessageBus{
		subs:   make(map[string]*localSubList), // initialize subs and queues
		queues: make(map[string]*localSubList),
	}
}

// The `Publish` function is used to publish a message to a channel in the local message bus. It takes
// in a context, a channel name, and a message of type `proto.Message`. The message is serialized using
// the `serialize` function and then published to the subscribers of the channel. The function first
// locks the `localMessageBus` using a read lock, then retrieves the list of subscribers and queue
// subscribers for the channel. It then unlocks the `localMessageBus` and publishes the message to the
// subscribers and queue subscribers if they exist.
func (l *localMessageBus) Publish(_ context.Context, channel string, msg proto.Message) error { // Publish publishes a message to a channel.
	b, err := serialize(msg) // serialize is a function that serializes a proto.Message to a byte slice.
	if err != nil {
		return err
	}

	l.RLock()                   // lock localMessageBus before localSubList
	subs := l.subs[channel]     // get the list of subscribers to the channel
	queues := l.queues[channel] // get the list of queue subscribers to the channel
	l.RUnlock()                 // unlock localMessageBus after localSubList

	if subs != nil {
		subs.publish(b)
	}
	if queues != nil {
		queues.publish(b)
	}
	return nil
}

// This function is implementing the `Subscribe` method of the `MessageBus` interface for the
// `localMessageBus` struct. It takes in a context, a channel name, and a size parameter, and returns a
// `subInternal` interface and an error. It calls the `subscribe` function with the `subs` map, the
// channel name, the size parameter, and `false` as the `queue` parameter. The `subscribe` function
// creates a new subscription to the channel and returns a `localSubscription` object that implements
// the `subInternal` interface. The `Subscribe` function returns this object and any error that
// occurred during subscription.
func (l *localMessageBus) Subscribe(_ context.Context, channel string, size int) (subInternal, error) {
	return l.subscribe(l.subs, channel, size, false)
}

// This function is implementing the `SubscribeQueue` method of the `MessageBus` interface for the
// `localMessageBus` struct. It takes in a context, a channel name, and a size parameter, and returns a
// `subInternal` interface and an error. It calls the `subscribe` function with the `queues` map, the
// channel name, the size parameter, and `true` as the `queue` parameter. The `subscribe` function
// creates a new subscription to the channel and returns a `localSubscription` object that implements
// the `subInternal` interface. The `SubscribeQueue` function returns this object and any error that
// occurred during subscription. This function is used to subscribe to a channel in the local message
// bus with a queue, where messages are delivered to each subscriber in the order they were received.
func (l *localMessageBus) SubscribeQueue(_ context.Context, channel string, size int) (subInternal, error) {
	return l.subscribe(l.queues, channel, size, true)
}

// The `subscribe` function is used to create a new subscription to a channel in the local message bus.
// It takes in a map of subscription lists (`subLists`), a channel name (`channel`), a size parameter
// (`size`), and a boolean flag indicating whether the subscription should use a queue (`queue`).
func (l *localMessageBus) subscribe(subLists map[string]*localSubList, channel string, size int, queue bool) (subInternal, error) {
	l.Lock()
	defer l.Unlock()

	subList := subLists[channel]
	if subList == nil {
		subList = &localSubList{queue: queue}
		subList.onUnsubscribe = func(index int) {
			// lock localMessageBus before localSubList
			l.Lock()
			subList.Lock()

			close(subList.subs[index])
			subList.subs[index] = nil
			subList.subCount--
			if subList.subCount == 0 {
				delete(subLists, channel)
			}

			subList.Unlock()
			l.Unlock()
		}
		subLists[channel] = subList
	}

	return subList.create(size), nil
}

// The type `localSubList` is a struct that contains a list of subscribers and related properties, with
// the ability to lock while holding a local message bus lock.
// @property  - This is a struct definition in Go programming language. It defines a type called
// `localSubList` which has the following properties:
// @property {[]chan []byte} subs - subs is a slice of channels of type []byte. This slice holds all
// the subscribers to a particular topic. When a message is published on this topic, it is sent to all
// the subscribers in this slice.
// @property {int} subCount - subCount is an integer that represents the number of subscribers
// currently subscribed to the localSubList.
// @property {bool} queue - The `queue` property is a boolean value that indicates whether messages
// should be queued for subscribers if they are not currently able to receive them. If `queue` is set
// to `true`, messages will be stored in a buffer until the subscriber is ready to receive them. If
// `queue` is set
// @property {int} next - The `next` property is an integer that represents the index of the next
// subscriber to be notified. It is used in conjunction with the `queue` property to determine whether
// messages should be queued for subscribers who are not currently ready to receive them. When a
// message is sent to the localSubList,
// @property onUnsubscribe - onUnsubscribe is a function that is called when a subscriber is removed
// from the localSubList. It takes an integer parameter that represents the index of the subscriber
// that was removed. This function can be used to perform any necessary cleanup or logging when a
// subscriber is unsubscribed.
type localSubList struct {
	sync.RWMutex  // locking while holding localMessageBus lock is allowed
	subs          []chan []byte
	subCount      int
	queue         bool
	next          int
	onUnsubscribe func(int)
}

// The `create` function is used to create a new subscription to a channel in the local message bus. It
// takes in a size parameter and returns a pointer to a `localSubscription` object. The function
// creates a new channel with the given size and adds it to the list of subscribers for the
// localSubList. If there is an empty slot in the list of subscribers, the new channel is added to that
// slot. Otherwise, a new slot is created at the end of the list. The function then returns a pointer
// to a `localSubscription` object that contains the new channel and an `onClose` function that is
// called when the subscription is closed. The `onClose` function removes the channel from the list of
// subscribers and calls the `onUnsubscribe` function of the localSubList.
func (l *localSubList) create(size int) *localSubscription {
	msgChan := make(chan []byte, size)

	l.Lock()
	defer l.Unlock()

	l.subCount++
	added := false
	index := 0
	for i, s := range l.subs {
		if s == nil {
			added = true
			index = i
			l.subs[i] = msgChan
			break
		}
	}

	if !added {
		index = len(l.subs)
		l.subs = append(l.subs, msgChan)
	}

	return &localSubscription{
		msgChan: msgChan,
		onClose: func() {
			l.onUnsubscribe(index)
		},
	}
}

// The `publish` function is used to publish a message to the subscribers of a channel in the local
// message bus. It takes in a byte slice `b` that represents the serialized message. If the `queue`
// property of the `localSubList` is set to `true`, the function locks the `localSubList` using a write
// lock and sends the message to the next subscriber in a round-robin fashion. If there are no
// subscribers available to receive the message, the message is not sent. If the `queue` property is
// set to `false`, the function locks the `localSubList` using a read lock and sends the message to all
// subscribers in the list. If a subscriber is not available to receive the message, the message is not
// sent to that subscriber.
func (l *localSubList) publish(b []byte) {
	if l.queue {
		l.Lock()
		defer l.Unlock()

		// round-robin
		for i := 0; i <= len(l.subs); i++ {
			if l.next >= len(l.subs) {
				l.next = 0
			}
			s := l.subs[l.next]
			l.next++
			if s != nil {
				s <- b
				return
			}
		}
	} else {
		l.RLock()
		defer l.RUnlock()

		// send to all
		for _, s := range l.subs {
			if s != nil {
				s <- b
			}
		}
	}
}

// The type `localSubscription` contains a channel for receiving messages and a function to be called
// when the subscription is closed.
// @property msgChan - msgChan is a channel of type []byte that is used to send and receive messages
// between the local subscription and the publisher.
// @property onClose - onClose is a function property of the localSubscription struct. It is a callback
// function that will be executed when the subscription is closed or terminated. This function can be
// used to perform any necessary cleanup or resource release operations.
type localSubscription struct {
	msgChan chan []byte
	onClose func()
}

// The `read` function is a method of the `localSubscription` struct. It reads a message from the
// subscription's message channel (`msgChan`) and returns the message as a byte slice and a boolean
// value indicating whether the channel is still open or has been closed. If the channel is closed, the
// function returns a `nil` byte slice and `false`. This function is used to read messages from a
// subscription in the local message bus.
func (l *localSubscription) read() ([]byte, bool) {
	msg, ok := <-l.msgChan
	if !ok {
		return nil, false
	}
	return msg, true
}

// The `Close()` function is a method of the `localSubscription` struct. It calls the `onClose()`
// function, which is a callback function that will be executed when the subscription is closed or
// terminated. This function can be used to perform any necessary cleanup or resource release
// operations. The `Close()` function then returns `nil` as there is no error to report.
func (l *localSubscription) Close() error {
	l.onClose()
	return nil
}
