package psrpc

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"
)

type localMessageBus struct { // localMessageBus is a MessageBus implementation that uses channels to send messages.
	sync.RWMutex // RWMutex is used to lock the localMessageBus while holding the localSubList lock is allowed
	subs         map[string]*localSubList
	queues       map[string]*localSubList
}

func NewLocalMessageBus() MessageBus {
	return &localMessageBus{
		subs:   make(map[string]*localSubList),
		queues: make(map[string]*localSubList),
	}
}

func (l *localMessageBus) Publish(_ context.Context, channel string, msg proto.Message) error {
	b, err := serialize(msg)
	if err != nil {
		return err
	}

	l.RLock()
	subs := l.subs[channel]
	queues := l.queues[channel]
	l.RUnlock()

	if subs != nil {
		subs.publish(b)
	}
	if queues != nil {
		queues.publish(b)
	}
	return nil
}

func (l *localMessageBus) Subscribe(_ context.Context, channel string, size int) (subInternal, error) {
	return l.subscribe(l.subs, channel, size, false)
}

func (l *localMessageBus) SubscribeQueue(_ context.Context, channel string, size int) (subInternal, error) {
	return l.subscribe(l.queues, channel, size, true)
}

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

type localSubList struct {
	sync.RWMutex  // locking while holding localMessageBus lock is allowed
	subs          []chan []byte
	subCount      int
	queue         bool
	next          int
	onUnsubscribe func(int)
}

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

type localSubscription struct {
	msgChan chan []byte
	onClose func()
}

func (l *localSubscription) read() ([]byte, bool) {
	msg, ok := <-l.msgChan
	if !ok {
		return nil, false
	}
	return msg, true
}

func (l *localSubscription) Close() error {
	l.onClose()
	return nil
}
