package psrpc

import (
	"context"

	"google.golang.org/protobuf/proto"
)

const (
	DefaultChannelSize = 100
)

// The MessageBus interface defines methods for publishing and subscribing to channels.
// @property {error} Publish - Publish is a method of the MessageBus interface that allows a client to
// publish a message to a specific channel. The method takes in a context, a channel name, and a
// message (which should be a protocol buffer message) as arguments. The method returns an error if
// there was an issue publishing the
// @property Subscribe - Subscribe is a method of the MessageBus interface that allows a client to
// subscribe to a specific channel and receive messages published to that channel. The method takes in
// a context.Context object, a string representing the channel to subscribe to, and an integer
// representing the size of the channel. The method returns a sub
// @property SubscribeQueue - SubscribeQueue is a method of the MessageBus interface that allows a
// subscriber to receive messages from a channel in a queue-like fashion. This means that messages are
// distributed evenly among all subscribers, rather than being sent to all subscribers at once. This
// can be useful in scenarios where multiple subscribers are listening to the
type MessageBus interface {
	Publish(ctx context.Context, channel string, msg proto.Message) error
	Subscribe(ctx context.Context, channel string, channelSize int) (subInternal, error)
	SubscribeQueue(ctx context.Context, channel string, channelSize int) (subInternal, error)
}

// The below type defines an interface with two methods, read and Close, for internal use within the Go
// program.
// @property read - read is a method that returns a byte slice and a boolean value. The byte slice
// contains the data read from a source, and the boolean value indicates whether there is more data to
// be read or not.
// @property {error} Close - Close is a method that takes no arguments and returns an error. It is used
// to close the resource associated with the subInternal interface. This method should be called when
// the resource is no longer needed to free up any system resources that it may be holding.
type subInternal interface {
	read() ([]byte, bool)
	Close() error
}

// The below type defines a generic subscription interface for a channel that receives messages of a
// specified type.
// @property Channel - The `Channel()` method returns a read-only channel of `MessageType`. This
// channel can be used to receive messages of type `MessageType` from the subscription.
// @property {error} Close - Close is a method that closes the subscription and releases any resources
// associated with it. It returns an error if there was a problem closing the subscription.
type Subscription[MessageType proto.Message] interface {
	Channel() <-chan MessageType
	Close() error
}

// The type `subscription` is a generic struct that contains an internal subscription and a read-only
// channel for a specific message type.
// @property {subInternal}  - This is a Go language struct definition for a subscription object. It has
// two properties:
// @property c - The "c" property is a channel of type "MessageType" that is used to receive messages
// of that type. This channel is used to deliver messages to subscribers who have subscribed to this
// particular subscription.
type subscription[MessageType proto.Message] struct {
	subInternal
	c <-chan MessageType
}

// This function is defining a method for the `subscription` struct that returns a read-only channel of
// `MessageType`. The channel `c` is a property of the `subscription` struct and is used to deliver
// messages to subscribers who have subscribed to this particular subscription. The method returns this
// channel to allow subscribers to receive messages of the specified type.
func (s *subscription[MessageType]) Channel() <-chan MessageType {
	return s.c
}

// The function `Subscribe` subscribes to a message bus channel and returns a subscription object.
func Subscribe[MessageType proto.Message](
	ctx context.Context,
	bus MessageBus,
	channel string,
	channelSize int,
) (Subscription[MessageType], error) {

	sub, err := bus.Subscribe(ctx, channel, channelSize)
	if err != nil {
		return nil, err
	}

	return toSubscription[MessageType](sub, channelSize), nil
}

// The function subscribes to a message queue and returns a subscription object.
func SubscribeQueue[MessageType proto.Message](
	ctx context.Context,
	bus MessageBus,
	channel string,
	channelSize int,
) (Subscription[MessageType], error) {

	sub, err := bus.SubscribeQueue(ctx, channel, channelSize)
	if err != nil {
		return nil, err
	}

	return toSubscription[MessageType](sub, channelSize), nil
}

// This function converts a subInternal object into a Subscription object that can receive and
// deserialize messages of a specified type.
func toSubscription[MessageType proto.Message](sub subInternal, size int) Subscription[MessageType] {
	msgChan := make(chan MessageType, size)
	go func() {
		for {
			b, ok := sub.read()
			if !ok {
				close(msgChan)
				return
			}

			p, err := deserialize(b)
			if err != nil {
				logger.Error(err, "failed to deserialize message")
				continue
			}
			msgChan <- p.(MessageType)
		}
	}()

	return &subscription[MessageType]{
		subInternal: sub,
		c:           msgChan,
	}
}

// The above code defines a generic type `nilSubscription` with no fields or methods.
type nilSubscription[MessageType any] struct{}

// The `Channel()` method of the `nilSubscription` struct is returning a `nil` channel of type
// `MessageType`. This is because the `nilSubscription` struct is a placeholder subscription object
// that is returned when there is an error subscribing to a channel. Since there is no actual
// subscription associated with this object, the `Channel()` method returns a `nil` channel to indicate
// that there are no messages to be received.
func (s nilSubscription[MessageType]) Channel() <-chan MessageType {
	return nil
}

// The `Close()` method of the `nilSubscription` struct is a no-op method that returns `nil`. This
// method is called when closing a subscription object, but since the `nilSubscription` struct is a
// placeholder object that is returned when there is an error subscribing to a channel, there is no
// actual subscription to close. Therefore, the `Close()` method simply returns `nil` to indicate that
// there is nothing to close.
func (s nilSubscription[MessageType]) Close() error {
	return nil
}
