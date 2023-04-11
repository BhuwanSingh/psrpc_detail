// The below code defines an RPC server in Go that allows for registering and deregistering handlers,
// publishing messages, and closing the server.
package psrpc

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/atomic"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"
)

// The above type defines an interface for a RPC handler with a method to close the handler.
// @property close - "close" is a method signature that belongs to an interface called "rpcHandler".
// The method takes a boolean parameter called "force" and has no return value. This interface is
// likely used in a larger program to define the behavior of objects that handle remote procedure calls
// (RPCs).
type rpcHandler interface {
	close(force bool)
}

// The RPCServer type represents a server that handles remote procedure calls and manages handlers for
// those calls.
// @property {serverOpts}  - - `serverOpts`: a struct containing various options for configuring the
// RPC server.
// @property {MessageBus} bus - The `bus` property is a `MessageBus` object that is used for
// communication between different components of the RPC server. It is likely used to send and receive
// messages between the server and its clients.
// @property {string} serviceName - The `serviceName` property is a string that represents the name of
// the RPC service. It is used to identify the service and distinguish it from other services that may
// be running on the same server.
// @property {string} id - The `id` property is a unique identifier for the RPC server instance.
// @property mu - The `mu` property is a `sync.RWMutex` type, which is a mutual exclusion lock that can
// be used to protect shared resources from concurrent access. It provides a way to allow multiple
// readers or a single writer to access a resource at the same time, while preventing multiple writers
// from accessing
// @property handlers - `handlers` is a map that stores the RPC handlers for the server. The keys of
// the map are the names of the RPC methods, and the values are the corresponding handler functions
// that will be executed when the method is called.
// @property active - The `active` property is an `atomic.Int32` variable that keeps track of the
// number of active RPC connections to the server. It is used to determine when the server can safely
// shut down.
// @property shutdown - `shutdown` is a channel used to signal the RPC server to shut down gracefully.
// When a value is sent to this channel, the server will stop accepting new requests and will wait for
// any active requests to complete before shutting down completely.
type RPCServer struct {
	serverOpts

	bus         MessageBus
	serviceName string
	id          string
	mu          sync.RWMutex
	handlers    map[string]rpcHandler
	active      atomic.Int32
	shutdown    chan struct{}
}

func NewRPCServer(serviceName, serverID string, bus MessageBus, opts ...ServerOption) *RPCServer {
	s := &RPCServer{
		serverOpts:  getServerOpts(opts...),
		bus:         bus,
		serviceName: serviceName,
		id:          serverID,
		handlers:    make(map[string]rpcHandler),
		shutdown:    make(chan struct{}),
	}

	return s
}

func RegisterHandler[RequestType proto.Message, ResponseType proto.Message](
	s *RPCServer,
	rpc string,
	topic []string,
	svcImpl func(context.Context, RequestType) (ResponseType, error),
	affinityFunc AffinityFunc[RequestType],
	requireClaim bool,
	multi bool,
) error {
	select {
	case <-s.shutdown:
		return errors.New("RPCServer closed")
	default:
	}

	key := getHandlerKey(rpc, topic)
	s.mu.RLock()
	_, ok := s.handlers[key]
	s.mu.RUnlock()
	if ok {
		return errors.New("handler already exists")
	}

	// create handler
	h, err := newRPCHandler(s, rpc, topic, svcImpl, s.chainedInterceptor, affinityFunc, requireClaim, multi)
	if err != nil {
		return err
	}

	s.active.Inc()
	h.onCompleted = func() {
		s.active.Dec()
		s.mu.Lock()
		delete(s.handlers, key)
		s.mu.Unlock()
	}

	s.mu.Lock()
	s.handlers[key] = h
	s.mu.Unlock()

	h.run(s)
	return nil
}

func RegisterStreamHandler[RequestType proto.Message, ResponseType proto.Message](
	s *RPCServer,
	rpc string,
	topic []string,
	svcImpl func(ServerStream[ResponseType, RequestType]) error,
	affinityFunc StreamAffinityFunc,
	requireClaim bool,
) error {
	select {
	case <-s.shutdown:
		return errors.New("RPCServer closed")
	default:
	}

	key := getHandlerKey(rpc, topic)
	s.mu.RLock()
	_, ok := s.handlers[key]
	s.mu.RUnlock()
	if ok {
		return errors.New("handler already exists")
	}

	// create handler
	h, err := newStreamRPCHandler(s, rpc, topic, svcImpl, s.chainedInterceptor, affinityFunc, requireClaim)
	if err != nil {
		return err
	}

	s.active.Inc()
	h.onCompleted = func() {
		s.active.Dec()
		s.mu.Lock()
		delete(s.handlers, key)
		s.mu.Unlock()
	}

	s.mu.Lock()
	s.handlers[key] = h
	s.mu.Unlock()

	h.run(s)
	return nil
}

func (s *RPCServer) DeregisterHandler(rpc string, topic []string) {
	key := getHandlerKey(rpc, topic)
	s.mu.RLock()
	h, ok := s.handlers[key]
	s.mu.RUnlock()
	if ok {
		h.close(true)
	}
}

func (s *RPCServer) Publish(ctx context.Context, rpc string, topic []string, msg proto.Message) error {
	return s.bus.Publish(ctx, getRPCChannel(s.serviceName, rpc, topic), msg)
}

func (s *RPCServer) Close(force bool) {
	select {
	case <-s.shutdown:
	default:
		close(s.shutdown)

		s.mu.RLock()
		handlers := maps.Values(s.handlers)
		s.mu.RUnlock()

		var wg sync.WaitGroup
		for _, h := range handlers {
			wg.Add(1)
			h := h
			go func() {
				h.close(force)
				wg.Done()
			}()
		}
		wg.Wait()
	}
	if !force {
		for s.active.Load() > 0 {
			time.Sleep(time.Millisecond * 100)
		}
	}
}
