package psrpc

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	// The `"github.com/livekit/psrpc/internal"` package is being imported in order to access internal
	// implementation details of the `psrpc` package. It is not intended for use by external users of the
	// `psrpc` package.
	"github.com/livekit/psrpc/internal"
)

// The Stream type is an interface that defines methods for sending and receiving messages over a
// channel, as well as closing the stream and accessing its context and error.
// @property Channel - The Channel method returns a receive-only channel of the type RecvType. This
// channel can be used to receive messages of type RecvType from the stream.
// @property {error} Send - Send is a method that sends a message of type SendType over the stream. It
// takes an optional list of StreamOption arguments that can be used to modify the behavior of the
// stream. If the message cannot be sent, an error is returned.
// @property {error} Close - The `Close` method is used to close the stream and indicate the reason for
// closure by passing an error as the `cause` parameter. This method should be called when the stream
// is no longer needed or when an error occurs that requires the stream to be closed. Once the stream
// is closed, no
// @property Context - The `Context()` method returns the context associated with the stream. This
// context can be used to carry deadlines, cancellation signals, and other request-scoped values across
// API boundaries and between processes. It is a way to pass request-scoped values, such as
// authentication credentials and tracing metadata, across API boundaries
// @property {error} Err - The `Err()` method returns the error that caused the stream to close, if
// any. If the stream is still open or was closed without an error, it returns `nil`.
type Stream[SendType, RecvType proto.Message] interface {
	Channel() <-chan RecvType
	Send(msg SendType, opts ...StreamOption) error
	Close(cause error) error
	Context() context.Context
	Err() error
}

// The ServerStream interface defines a streaming protocol with the ability to hijack the connection.
// @property Hijack - Hijack is a method that allows the caller to take over the underlying network
// connection of the server stream. This can be useful in certain scenarios where the caller needs more
// control over the connection, such as implementing custom load balancing or routing logic. Once the
// connection is hijacked, the server stream can
type ServerStream[SendType, RecvType proto.Message] interface {
	Stream[SendType, RecvType]
	Hijack()
}

// The below type defines a client stream interface with generic types for sending and receiving
// messages.
type ClientStream[SendType, RecvType proto.Message] interface {
	Stream[SendType, RecvType]
}

// The streamAdapter interface defines methods for sending and closing a stream.
// @property {error} send - send is a method that takes a context and a pointer to an internal Stream
// object as input and returns an error. It is used to send a stream message to a recipient.
// @property close - `close` is a method that takes a `streamID` as input and is responsible for
// closing the corresponding stream. It is not specified what happens when a stream is closed, but it
// is likely that any resources associated with the stream are released and any pending messages are
// discarded.
type streamAdapter interface {
	send(ctx context.Context, msg *internal.Stream) error
	close(streamID string)
}

// The type clientStream represents a client stream for an RPC client in Go.
// @property c - c is a pointer to an RPCClient struct, which likely represents a client that can make
// remote procedure calls to a server.
// @property {RPCInfo} info - The `info` property is of type `RPCInfo` and is a struct that contains
// information about the remote procedure call (RPC) being made, such as the name of the method being
// called, the arguments being passed, and any metadata associated with the call.
type clientStream struct {
	c    *RPCClient
	info RPCInfo
}

// This function is used to send a stream message to a recipient. It takes a context and a pointer to
// an internal Stream object as input and returns an error. It uses the `bus.Publish` method to publish
// the message to a channel that is specific to the service, method, and topic being called. If an
// error occurs during the publishing process, it returns a new error with the `Internal` error code.
func (s *clientStream) send(ctx context.Context, msg *internal.Stream) (err error) {
	if err = s.c.bus.Publish(ctx, getStreamServerChannel(s.c.serviceName, s.info.Method, s.info.Topic), msg); err != nil {
		err = NewError(Internal, err)
	}
	return
}

// This function is used to close a client stream identified by the given `streamID`. It acquires a
// lock on the `streamChannels` map of the `RPCClient` struct pointed to by `s.c`, deletes the entry
// corresponding to the given `streamID`, and releases the lock. This ensures that the `streamChannels`
// map is accessed in a thread-safe manner.
func (s *clientStream) close(streamID string) {
	s.c.mu.Lock()
	delete(s.c.streamChannels, streamID)
	s.c.mu.Unlock()
}

// The serverStream type is a generic struct used for handling streaming RPC requests and responses in
// a Go RPC server.
// @property h - h is a pointer to an instance of the streamRPCHandlerImpl struct, which is a type that
// handles the stream RPC requests and responses for a specific message type. It contains methods for
// sending and receiving messages over the stream.
// @property {string} nodeID - The nodeID property is a string that represents the unique identifier of
// the node that the server stream is associated with. This identifier is typically used to route
// messages and requests to the correct node in a distributed system.
type serverStream[SendType, RecvType proto.Message] struct {
	h      *streamRPCHandlerImpl[SendType, RecvType]
	s      *RPCServer
	nodeID string
}

// `func (s *serverStream[RequestType, ResponseType]) send(ctx context.Context, msg *internal.Stream)
// (err error)` is a method of the `serverStream` type. It is used to send a stream message to a
// recipient by publishing the message to a channel that is specific to the service and node ID being
// called. It takes a context and a pointer to an internal Stream object as input and returns an error.
// If an error occurs during the publishing process, it returns a new error with the `Internal` error
// code.
func (s *serverStream[RequestType, ResponseType]) send(ctx context.Context, msg *internal.Stream) (err error) {
	if err = s.s.bus.Publish(ctx, getStreamChannel(s.s.serviceName, s.nodeID), msg); err != nil {
		err = NewError(Internal, err)
	}
	return
}

// The below code is defining a method called `close` for a `serverStream` struct with generic types
// `RequestType` and `ResponseType`. This method takes a `streamID` parameter and removes the
// corresponding stream from the `streams` map in the `serverStream`'s `h` field. The `h` field is
// assumed to be a struct that contains a `mu` field, which is a mutex used to synchronize access to
// the `streams` map.
func (s *serverStream[RequestType, ResponseType]) close(streamID string) {
	s.h.mu.Lock()
	delete(s.h.streams, streamID)
	s.h.mu.Unlock()
}

// The type is a generic stream interceptor root that extends a stream implementation.
// @property {streamImpl}  - This is a struct definition in Go programming language. It defines a type
// `streamInterceptorRoot` which has two generic type parameters `SendType` and `RecvType` and one
// non-generic type parameter `proto.Message`.
type streamInterceptorRoot[SendType, RecvType proto.Message] struct {
	*streamImpl[SendType, RecvType]
}

// The below code is defining a method called `Recv` for a struct `streamInterceptorRoot` with generic
// types `SendType` and `RecvType`. The method takes a parameter `msg` of type `proto.Message` and
// returns an error. The method calls the `recv` function with the `msg` parameter and returns the
// result.
func (h *streamInterceptorRoot[SendType, RecvType]) Recv(msg proto.Message) error {
	return h.recv(msg)
}

// The below code is defining a method called `Send` for a struct `streamInterceptorRoot` with generic
// types `SendType` and `RecvType`. The method takes a `proto.Message` and optional `StreamOption`
// arguments and returns an error. The implementation of the method simply calls the `send` method of
// the `streamInterceptorRoot` struct with the provided message.
func (h *streamInterceptorRoot[SendType, RecvType]) Send(msg proto.Message, opts ...StreamOption) error {
	return h.send(msg)
}

// The below code is defining a method called `Close` for a struct type `streamInterceptorRoot` with
// generic types `SendType` and `RecvType`. The method takes an error `cause` as input and returns an
// error. The method calls another method `close` with the `cause` error as input and returns the
// result of that method call.
func (h *streamInterceptorRoot[SendType, RecvType]) Close(cause error) error {
	return h.close(cause)
}

// This is a struct type for a stream implementation in Go with various fields for managing the stream.
// @property {streamOpts}  - - `streamImpl` is a struct type.
// @property {streamAdapter} adapter - The adapter is a component that handles the low-level details of
// sending and receiving data over the network. It abstracts away the underlying transport protocol and
// provides a simple interface for sending and receiving messages. The streamImpl struct uses an
// adapter to communicate with the remote endpoint.
// @property {StreamInterceptor} interceptor - The `interceptor` property is a function that can be
// used to intercept and modify the behavior of the stream. It takes in a `context.Context` and a
// `RecvType` as input parameters and returns a `RecvType` and an `error`. It can be used to add
// additional functionality
// @property ctx - The context.Context object is used to carry deadlines, cancellation signals, and
// other request-scoped values across API boundaries and between processes. It allows for the
// propagation of request-scoped values across multiple API calls and allows for the cancellation of
// requests and associated resources. In this case, it is used to manage
// @property cancelCtx - The `cancelCtx` property is a function that can be called to cancel the
// context associated with the stream. This is useful for stopping any ongoing operations associated
// with the stream, such as sending or receiving messages.
// @property recvChan - `recvChan` is a channel used to receive messages of type `RecvType` from the
// stream. It is likely used in conjunction with the `adapter` and `interceptor` to handle incoming
// messages and perform any necessary processing or validation. The channel is likely buffered to allow
// for multiple messages to
// @property {string} streamID - The streamID property is a string that represents the unique
// identifier for the stream. It is used to identify the stream when sending and receiving messages.
// @property mu - The `mu` property is a `sync.Mutex` type variable used for synchronization purposes.
// It is used to ensure that only one goroutine can access the critical section of code at a time,
// preventing race conditions and ensuring thread safety. In this case, it is likely used to protect
// access to shared
// @property {bool} hijacked - The "hijacked" property is a boolean flag that indicates whether the
// stream has been hijacked or not. In the context of network programming, hijacking refers to taking
// control of a connection or stream that was originally intended for a different purpose. In this
// case, it likely refers to a situation
// @property pending - `pending` is an `atomic.Int32` variable that keeps track of the number of
// pending messages in the stream. It is incremented when a message is sent and decremented when a
// message is received or an error occurs. This variable is useful for flow control and for determining
// when the stream is idle
// @property acks - `acks` is a map that stores channels used for acknowledging messages received on
// the stream. When a message is received, the corresponding channel is signaled to indicate that the
// message has been processed. This is typically used in scenarios where the sender needs to ensure
// that the receiver has processed a message before sending the
// @property {error} err - `err` is a property of the `streamImpl` struct which is used to store any
// error that occurs during the stream's operation. It can be accessed and modified by the methods of
// the struct to handle errors and propagate them to the caller.
// @property closed - `closed` is a boolean flag that indicates whether the stream has been closed or
// not. It is implemented as an atomic boolean to ensure thread-safety. When the stream is closed, this
// flag is set to true and all subsequent operations on the stream will fail.
type streamImpl[SendType, RecvType proto.Message] struct {
	streamOpts
	adapter     streamAdapter
	interceptor StreamInterceptor
	ctx         context.Context
	cancelCtx   context.CancelFunc
	recvChan    chan RecvType
	streamID    string
	mu          sync.Mutex
	hijacked    bool
	pending     atomic.Int32
	acks        map[string]chan struct{}
	err         error
	closed      atomic.Bool
}

// The below code is a method in a Go struct that handles incoming messages on a stream. It switches on
// the type of message received and performs different actions accordingly.
func (s *streamImpl[SendType, RecvType]) handleStream(is *internal.Stream) error {
	switch b := is.Body.(type) {
	// The below code is a case statement in a Go program that handles a specific type of message received
	// on a stream. It first acquires a lock on a mutex to ensure thread safety. It then checks if there
	// is an entry in a map called "acks" with the key equal to the request ID of the received message. If
	// there is, it retrieves the corresponding value (a channel) and removes the entry from the map. It
	// then releases the lock on the mutex. Finally, if the entry was found, it closes the channel. This
	// code is likely part of a larger program that uses channels
	case *internal.Stream_Ack:
		s.mu.Lock()
		ack, ok := s.acks[is.RequestId]
		delete(s.acks, is.RequestId)
		s.mu.Unlock()

		if ok {
			close(ack)
		}

	// The below code is a case statement in a Go program that handles a message received on a stream. It
	// first checks if the stream is closed and returns an error if it is. It then increments a counter
	// for pending messages and defers decrementing it until the end of the function. The payload of the
	// message is deserialized using a function specified by the RecvType variable. If there is an error
	// during deserialization, it is wrapped in a custom error and the stream is closed using an
	// interceptor. If there is no error, the payload is passed to the interceptor's Recv method. The
	case *internal.Stream_Message:
		if s.closed.Load() {
			return ErrStreamClosed
		}

		s.pending.Inc()
		defer s.pending.Dec()

		v, err := deserializePayload[RecvType](b.Message.RawMessage, b.Message.Message)
		if err != nil {
			err = NewError(MalformedRequest, err)
			_ = s.interceptor.Close(err)
			return err
		}

		if err := s.interceptor.Recv(v); err != nil {
			return err
		}

		ctx, cancel := context.WithDeadline(s.ctx, time.Unix(0, is.Expiry))
		defer cancel()
		if err := s.ack(ctx, is); err != nil {
			return err
		}

	// The below code is a case statement in a Go program that handles the `internal.Stream_Close`
	// message. It checks if the stream is already closed and if not, it sets the `closed` flag to true,
	// sets the error if there is one, closes the adapter, cancels the context, and closes the receive
	// channel. This code is likely part of a larger program that manages streams and handles various
	// messages related to stream management.
	case *internal.Stream_Close:
		if !s.closed.Swap(true) {
			s.mu.Lock()
			s.err = newErrorFromResponse(b.Close.Code, b.Close.Error)
			s.mu.Unlock()

			s.adapter.close(s.streamID)
			s.cancelCtx()
			close(s.recvChan)
		}
	}

	return nil
}

// The below code is a method implementation in Go language for receiving messages of type `RecvType`
// in a stream. It uses a channel `recvChan` to receive the message and returns an error
// `ErrSlowConsumer` if the channel is blocked due to a slow consumer. The method takes a parameter
// `msg` of type `proto.Message` which is type asserted to `RecvType`.
func (s *streamImpl[SendType, RecvType]) recv(msg proto.Message) error {
	select {
	case s.recvChan <- msg.(RecvType):
	default:
		return ErrSlowConsumer
	}
	return nil
}

// The below code is defining a method called `waitForPending()` for a struct called `streamImpl` with
// two generic types `SendType` and `RecvType`. This method waits for any pending operations to
// complete by checking the value of a `pending` variable (which is likely a counter) until it reaches
// zero. It does this by repeatedly sleeping for 100 milliseconds until the pending count is zero.
func (s *streamImpl[SendType, RecvType]) waitForPending() {
	for s.pending.Load() > 0 {
		time.Sleep(time.Millisecond * 100)
	}
}

// The below code is implementing the `ack` method for a `streamImpl` struct in Go. This method sends
// an acknowledgement message to the server using the `adapter` object's `send` method. The
// acknowledgement message contains the `StreamId`, `RequestId`, `SentAt`, and `Expiry` fields from the
// original `Stream` object, and an empty `StreamAck` object in the `Body` field.
func (s *streamImpl[SendType, RecvType]) ack(ctx context.Context, is *internal.Stream) error {
	return s.adapter.send(ctx, &internal.Stream{
		StreamId:  is.StreamId,
		RequestId: is.RequestId,
		SentAt:    is.SentAt,
		Expiry:    is.Expiry,
		Body: &internal.Stream_Ack{
			Ack: &internal.StreamAck{},
		},
	})
}

// The below code is implementing the `close` method for a stream in Go. It first checks if the stream
// is already closed and returns an error if it is. Then, it sets the error cause for the stream and
// creates a message to send to the adapter indicating that the stream is closed. The message includes
// an error code and message if the cause is an `Error` type. The method then sends the message to the
// adapter, waits for any pending requests to complete, closes the stream adapter, cancels the context,
// and closes the receive channel. Finally, it returns any error that occurred while
func (s *streamImpl[RequestType, ResponseType]) close(cause error) error {
	if s.closed.Swap(true) {
		return ErrStreamClosed
	}

	if cause == nil {
		cause = ErrStreamClosed
	}

	// The below code is acquiring a lock on a mutex object `s.mu`. This is a synchronization mechanism
	// used in Go to prevent multiple goroutines from accessing shared resources simultaneously. Once the
	// lock is acquired, any other goroutine that tries to acquire the same lock will be blocked until the
	// lock is released.
	s.mu.Lock()
	// The below code is assigning the value of the variable `cause` to the `err` field of the struct `s`.
	s.err = cause
	// The below code is releasing the lock on a mutex (short for mutual exclusion) in Go. Mutexes are
	// used to synchronize access to shared resources in concurrent programming to prevent race
	// conditions. The `Unlock()` method is used to release the lock on the mutex after it has been
	// acquired with the `Lock()` method.
	s.mu.Unlock()

	// The below code is declaring a variable `msg` of type `*internal.StreamClose`, which is a pointer to
	// a struct named `StreamClose` defined in the `internal` package. The `&` operator is used to get the
	// memory address of the newly created `StreamClose` struct and assign it to the `msg` variable.
	msg := &internal.StreamClose{}
	// The below code is checking if the `cause` error implements the `Error` interface and if it does, it
	// extracts the error message and error code from it and assigns them to `msg.Error` and `msg.Code`
	// respectively. If the `cause` error does not implement the `Error` interface, it assigns the error
	// message to `msg.Error` and assigns the `Unknown` error code to `msg.Code`.
	var e Error
	if errors.As(cause, &e) {
		msg.Error = e.Error()
		msg.Code = string(e.Code())
	} else {
		msg.Error = cause.Error()
		msg.Code = string(Unknown)
	}

	// The below code in Go is getting the current date and time and storing it in the variable `now`.
	now := time.Now()
	// The below code is sending a message to a stream using the `send` method of an adapter. The message
	// being sent is a `Stream_Close` message with the content of `msg`. The message is being sent with a
	// specific `StreamId`, `RequestId`, `SentAt` timestamp, and `Expiry` timestamp. The `timeout` value
	// is also being used to calculate the `Expiry` timestamp.
	err := s.adapter.send(context.Background(), &internal.Stream{
		StreamId:  s.streamID,
		RequestId: newRequestID(),
		SentAt:    now.UnixNano(),
		Expiry:    now.Add(s.timeout).UnixNano(),
		Body: &internal.Stream_Close{
			Close: msg,
		},
	})

	// The below code is calling a method `waitForPending()` on an object `s`. It is likely that
	// `waitForPending()` is a method that waits for any pending tasks or operations to complete before
	// proceeding with the rest of the code. The purpose of this code is to ensure that all pending tasks
	// are completed before continuing with the execution of the program.
	s.waitForPending()
	// The below code is closing a stream with the given `streamID` using the `close` method of the
	// `adapter` object. It is likely part of a larger program or function that manages network
	// connections or data streams.
	s.adapter.close(s.streamID)
	// The below code is calling the `cancelCtx()` method on an object named `s`. This method is likely
	// used to cancel a context in Go, which is a way to manage the lifecycle of a request or operation.
	// By calling `cancelCtx()`, the context is cancelled and any associated resources can be cleaned up.
	s.cancelCtx()
	// The below code is closing the channel `s.recvChan`. This means that no more values can be sent on
	// the channel and any attempt to do so will result in a panic.
	close(s.recvChan)

	return err
}

// The below code is a method implementation in Go that sends a message over a stream. It serializes
// the message payload, creates a request ID, sets a deadline for the request, and sends the message
// using an adapter. It then waits for an acknowledgement from the receiver or for a timeout to occur.
// If an acknowledgement is received, the method returns without error. If a timeout or other error
// occurs, an error is returned.
func (s *streamImpl[SendType, RecvType]) send(msg proto.Message, opts ...StreamOption) (err error) {
	// The below code is incrementing the value of a variable called `pending` by 1. The `Inc()` method is
	// likely a method of a struct or type that has a field called `pending`.
	s.pending.Inc()
	// The below code is using the `defer` keyword in Go to schedule a function call to `s.pending.Dec()`
	// to be executed when the surrounding function returns.
	defer s.pending.Dec()

	o := getStreamOpts(s.streamOpts, opts...)

	b, a, err := serializePayload(msg)
	if err != nil {
		err = NewError(MalformedRequest, err)
		return
	}

	ackChan := make(chan struct{})
	requestID := newRequestID()

	s.mu.Lock()
	s.acks[requestID] = ackChan
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.acks, requestID)
		s.mu.Unlock()
	}()

	now := time.Now()
	deadline := now.Add(o.timeout)

	ctx, cancel := context.WithDeadline(s.ctx, deadline)
	defer cancel()

	err = s.adapter.send(ctx, &internal.Stream{
		StreamId:  s.streamID,
		RequestId: requestID,
		SentAt:    now.UnixNano(),
		Expiry:    deadline.UnixNano(),
		Body: &internal.Stream_Message{
			Message: &internal.StreamMessage{
				Message:    a,
				RawMessage: b,
			},
		},
	})
	if err != nil {
		return
	}

	select {
	case <-ackChan:
	case <-ctx.Done():
		err = ErrRequestTimedOut
	case <-s.ctx.Done():
		err = s.Err()
	}

	return
}

func (s *streamImpl[SendType, RecvType]) Context() context.Context {
	return s.ctx
}

func (s *streamImpl[SendType, RecvType]) Hijacked() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hijacked
}

func (s *streamImpl[SendType, RecvType]) Hijack() {
	s.mu.Lock()
	s.hijacked = true
	s.mu.Unlock()
}

func (s *streamImpl[SendType, RecvType]) Channel() <-chan RecvType {
	return s.recvChan
}

func (s *streamImpl[SendType, RecvType]) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *streamImpl[RequestType, ResponseType]) Close(cause error) error {
	return s.interceptor.Close(cause)
}

func (s *streamImpl[SendType, RecvType]) Send(request SendType, opts ...StreamOption) (err error) {
	return s.interceptor.Send(request, opts...)
}
