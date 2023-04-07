package psrpc

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/livekit/psrpc/internal"
)

func TestMessageBus(t *testing.T) {
	// This is a test case for the `Local` message bus implementation. It creates a new instance of the
	// `LocalMessageBus`, and then runs three tests: `testSubscribe`, `testSubscribeQueue`, and
	// `testSubscribeClose`, passing the `bus` instance as a parameter to each of them. These tests verify
	// the functionality of the `Subscribe`, `SubscribeQueue`, and `Close` methods of the
	// `LocalMessageBus`.
	t.Run("Local", func(t *testing.T) {
		bus := NewLocalMessageBus()
		testSubscribe(t, bus)
		testSubscribeQueue(t, bus)
		testSubscribeClose(t, bus)
	})

	// This code block is a test case for the Redis message bus implementation. It creates a Redis client
	// using the `go-redis` library, creates a new instance of the `RedisMessageBus` using the client, and
	// then runs three tests: `testSubscribe`, `testSubscribeQueue`, and `testSubscribeClose`, passing the
	// `bus` instance as a parameter to each of them. These tests verify the functionality of the
	// `Subscribe`, `SubscribeQueue`, and `Close` methods of the `RedisMessageBus`.
	t.Run("Redis", func(t *testing.T) {
		rc := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{"localhost:6379"}})
		bus := NewRedisMessageBus(rc)
		testSubscribe(t, bus)
		testSubscribeQueue(t, bus)
		testSubscribeClose(t, bus)
	})

	t.Run("Nats", func(t *testing.T) {
		nc, _ := nats.Connect(nats.DefaultURL)
		bus := NewNatsMessageBus(nc)
		testSubscribe(t, bus)
		testSubscribeQueue(t, bus)
		testSubscribeClose(t, bus)
	})
}

// The function tests subscribing to a message bus and publishing a message to a channel.
func testSubscribe(t *testing.T, bus MessageBus) {
	// `ctx := context.Background()` is creating a new empty context that can be used to pass cancellation
	// signals and deadlines to functions that support it. In this code, it is used to pass the context to
	// the `Subscribe` and `Publish` methods of the message bus implementations.
	ctx := context.Background()

	channel := newID()
	subA, err := Subscribe[*internal.Request](ctx, bus, channel, DefaultChannelSize)
	require.NoError(t, err)
	subB, err := Subscribe[*internal.Request](ctx, bus, channel, DefaultChannelSize)
	require.NoError(t, err)
	time.Sleep(time.Millisecond * 100)

	require.NoError(t, bus.Publish(ctx, channel, &internal.Request{
		RequestId: "1",
	}))

	msgA := <-subA.Channel()
	msgB := <-subB.Channel()
	require.NotNil(t, msgA)
	require.NotNil(t, msgB)
	require.Equal(t, "1", msgA.RequestId)
	require.Equal(t, "1", msgB.RequestId)
}

func testSubscribeQueue(t *testing.T, bus MessageBus) {
	ctx := context.Background()

	channel := newID()
	subA, err := SubscribeQueue[*internal.Request](ctx, bus, channel, DefaultChannelSize)
	require.NoError(t, err)
	subB, err := SubscribeQueue[*internal.Request](ctx, bus, channel, DefaultChannelSize)
	require.NoError(t, err)
	time.Sleep(time.Millisecond * 100)

	require.NoError(t, bus.Publish(ctx, channel, &internal.Request{
		RequestId: "2",
	}))

	received := 0
	select {
	case m := <-subA.Channel():
		if m != nil {
			received++
		}
	case <-time.After(DefaultClientTimeout):
		// continue
	}

	select {
	case m := <-subB.Channel():
		if m != nil {
			received++
		}
	case <-time.After(DefaultClientTimeout):
		// continue
	}

	require.Equal(t, 1, received)
}

func testSubscribeClose(t *testing.T, bus MessageBus) {
	ctx := context.Background()

	channel := newID()
	sub, err := Subscribe[*internal.Request](ctx, bus, channel, DefaultChannelSize)
	require.NoError(t, err)

	sub.Close()
	time.Sleep(time.Millisecond * 100)

	select {
	case _, ok := <-sub.Channel():
		require.False(t, ok)
	default:
		require.FailNow(t, "closed subscription channel should not block")
	}
}
