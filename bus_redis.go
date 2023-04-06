// The code defines a Redis message bus implementation with methods for publishing and subscribing to
// channels, including queue mode with message locking.
package psrpc

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

// `const lockExpiration = time.Second * 5` is defining a constant variable `lockExpiration` with a
// value of 5 seconds, represented as a `time.Duration` type. This constant is used as the expiration
// time for a lock that is acquired when subscribing to a Redis channel in queue mode. The lock ensures
// that only one subscriber processes a message at a time, preventing multiple subscribers from
// processing the same message simultaneously. The lock is released after 5 seconds, allowing another
// subscriber to acquire it and process the next message.
const lockExpiration = time.Second * 5

// The redisMessageBus type represents a Redis message bus with a redis.UniversalClient.
// @property rc - rc is a property of type redis.UniversalClient which represents a Redis client that
// can be used to interact with a Redis server. It can be used to send and receive messages over a
// Redis message bus.
type redisMessageBus struct {
	rc redis.UniversalClient
}

// The function creates a new Redis message bus using a Redis client.
func NewRedisMessageBus(rc redis.UniversalClient) MessageBus {
	return &redisMessageBus{
		rc: rc,
	}
}

// The `Publish` function is a method of the `redisMessageBus` type that implements the `Publish`
// method of the `MessageBus` interface. It takes a `context.Context` object, a string representing the
// channel to publish the message to, and a `proto.Message` object representing the message to be
// published. The function first serializes the message using the `serialize` function (not shown in
// the code snippet), and then publishes the serialized message to the specified channel using the
// Redis client's `Publish` method. If there is an error during serialization or publishing, the
// function returns the error.
func (r *redisMessageBus) Publish(ctx context.Context, channel string, msg proto.Message) error {
	b, err := serialize(msg)
	if err != nil {
		return err
	}

	return r.rc.Publish(ctx, channel, b).Err()
}

// The `Subscribe` function is a method of the `redisMessageBus` type that implements the `Subscribe`
// method of the `MessageBus` interface. It takes a `context.Context` object, a string representing the
// channel to subscribe to, and an integer representing the size of the channel buffer. The function
// creates a new Redis subscription using the Redis client's `Subscribe` method, and then returns a new
// `redisSubscription` object that implements the `subInternal` interface. The `redisSubscription`
// object has a `msgChan` property that is set to the Redis subscription's channel with the specified
// buffer size. The `msgChan` channel is used to receive messages from the Redis channel. If there is
// an error during subscription, the function returns the error.
func (r *redisMessageBus) Subscribe(ctx context.Context, channel string, size int) (subInternal, error) {
	sub := r.rc.Subscribe(ctx, channel)
	return &redisSubscription{
		sub:     sub,
		msgChan: sub.Channel(redis.WithChannelSize(size)),
	}, nil
}

// The `SubscribeQueue` function is a method of the `redisMessageBus` type that implements the
// `SubscribeQueue` method of the `MessageBus` interface. It takes a `context.Context` object, a string
// representing the channel to subscribe to, and an integer representing the size of the channel
// buffer. The function creates a new Redis subscription using the Redis client's `Subscribe` method,
// and then returns a new `redisSubscription` object that implements the `subInternal` interface. The
// `redisSubscription` object has a `msgChan` property that is set to the Redis subscription's channel
// with the specified buffer size. The `msgChan` channel is used to receive messages from the Redis
// channel. Additionally, the `queue` property of the `redisSubscription` object is set to `true`,
// indicating that the subscription is in queue mode. In queue mode, a lock is acquired when a message
// is received to ensure that only one subscriber processes the message at a time. The lock is released
// after a specified expiration time, allowing another subscriber to acquire it and process the next
// message.
func (r *redisMessageBus) SubscribeQueue(ctx context.Context, channel string, size int) (subInternal, error) {
	sub := r.rc.Subscribe(ctx, channel)
	return &redisSubscription{
		ctx:     ctx,
		rc:      r.rc,
		sub:     sub,
		msgChan: sub.Channel(redis.WithChannelSize(size)),
		queue:   true,
	}, nil
}

// The redisSubscription type represents a subscription to a Redis Pub/Sub channel with associated
// context, client, message channel, and queue flag.
// @property ctx - ctx is a context.Context object that is used to manage the lifecycle of the
// subscription. It can be used to cancel the subscription or to set deadlines for the subscription.
// @property rc - rc is a redis client that implements the UniversalClient interface. It can be used to
// interact with a Redis server and perform various operations such as setting and getting values,
// subscribing to channels, and publishing messages.
// @property sub - `sub` is a pointer to a `redis.PubSub` object, which is used for subscribing to
// Redis channels and receiving messages published on those channels. It allows the client to subscribe
// to one or more channels and receive messages asynchronously as they are published.
// @property msgChan - msgChan is a channel that receives messages published to a Redis channel
// subscribed by the redisSubscription struct. The channel is used to receive messages asynchronously
// and can be used to process the messages in a non-blocking way.
// @property {bool} queue - The `queue` property is a boolean value that indicates whether the
// subscription is a queue subscription or not. If `queue` is set to `true`, then the subscription will
// receive messages in a first-in, first-out (FIFO) order. If `queue` is set to `false`,
type redisSubscription struct {
	ctx     context.Context
	rc      redis.UniversalClient
	sub     *redis.PubSub
	msgChan <-chan *redis.Message
	queue   bool
}

// The `read` function is a method of the `redisSubscription` type that reads messages from the Redis
// subscription's message channel (`msgChan`). If the subscription is in queue mode (`r.queue` is
// `true`), the function acquires a lock using Redis's `SetNX` method to ensure that only one
// subscriber processes the message at a time. The lock is released after a specified expiration time
// (`lockExpiration`), allowing another subscriber to acquire it and process the next message. The
// function returns the message payload as a byte slice and a boolean value indicating whether a
// message was successfully read (`true`) or if the message channel was closed (`false`).
func (r *redisSubscription) read() ([]byte, bool) {
	for {
		msg, ok := <-r.msgChan
		if !ok {
			return nil, false
		}

		if r.queue {
			sha := sha256.Sum256([]byte(msg.Payload))
			hash := base64.StdEncoding.EncodeToString(sha[:])
			acquired, err := r.rc.SetNX(r.ctx, hash, rand.Int(), lockExpiration).Result()
			if err != nil || !acquired {
				continue
			}
		}

		return []byte(msg.Payload), true
	}
}

// The `Close` function is a method of the `redisSubscription` type that closes the Redis subscription
// by calling the `Close` method of the `redis.PubSub` object (`r.sub`). It returns an error if there
// was an error closing the subscription.
func (r *redisSubscription) Close() error {
	return r.sub.Close()
}
