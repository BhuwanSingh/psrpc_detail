// The below code defines a Go package for implementing a client for a remote procedure call (RPC)
// system, including functions for making single and multi requests, joining and opening streams, and
// handling errors.
package psrpc

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/livekit/psrpc/internal"
)

// The below code is defining several error variables in Go, each with a specific error message and
// error code. These errors are related to network requests and streaming, and include errors for
// canceled requests, timed out requests, no response from servers, end of stream, closed stream, and
// slow consumer. The errors are created using the NewError function, which takes an error code and an
// error message as arguments.
var (
	ErrRequestCanceled = NewError(Canceled, errors.New("request canceled"))
	ErrRequestTimedOut = NewError(DeadlineExceeded, errors.New("request timed out"))
	ErrNoResponse      = NewError(Unavailable, errors.New("no response from servers"))
	ErrStreamEOF       = NewError(Unavailable, io.EOF)
	ErrStreamClosed    = NewError(Canceled, errors.New("stream closed"))
	ErrSlowConsumer    = NewError(Unavailable, errors.New("stream message discarded by slow consumer"))
)

// The function creates a new RPC client with specified options and subscribes to message channels for
// responses, claim requests, and streams.
func NewRPCClient(serviceName, clientID string, bus MessageBus, opts ...ClientOption) (*RPCClient, error) {
	// The below code is creating a new instance of an RPCClient struct in the Go programming language.
	// The struct has several fields including clientOpts, bus, serviceName, id, claimRequests,
	// responseChannels, streamChannels, and closed. The values for some of these fields are being
	// initialized using the make function to create maps and channels.
	c := &RPCClient{
		clientOpts:       getClientOpts(opts...),
		bus:              bus,
		serviceName:      serviceName,
		id:               clientID,
		claimRequests:    make(map[string]chan *internal.ClaimRequest),
		responseChannels: make(map[string]chan *internal.Response),
		streamChannels:   make(map[string]chan *internal.Stream),
		closed:           make(chan struct{}),
	}

	// The below code is creating a new context object with an empty context value. The context package in
	// Go is used to manage cancellation signals, deadlines, and other request-scoped values across API
	// boundaries and between processes. The `context.Background()` function returns an empty context that
	// has no values associated with it.
	ctx := context.Background()
	// The below code is subscribing to a channel for receiving responses of type `*internal.Response`
	// using the `Subscribe` function. The function takes in the context `ctx`, a message bus `c.bus`, the
	// name of the response channel `getResponseChannel(serviceName, clientID)`, and the size of the
	// channel `c.channelSize`. The function returns a channel of responses and an error if any.
	responses, err := Subscribe[*internal.Response](
		ctx, c.bus, getResponseChannel(serviceName, clientID), c.channelSize,
	)
	// The below code is checking if the variable `err` is not equal to `nil`. If `err` is not `nil`, it
	// returns `nil` and the value of `err`. This is commonly used in Go to handle errors that may occur
	// during the execution of a function.
	if err != nil {
		return nil, err
	}

	// The below code is subscribing to a channel that receives `ClaimRequest` messages. It is using the
	// `Subscribe` function from the `internal` package to subscribe to the channel. The function takes in
	// the context, the bus, the channel name, and the channel size as parameters. The
	// `getClaimRequestChannel` function is used to generate the channel name based on the service name
	// and client ID. The `Subscribe` function returns a channel of `ClaimRequest` messages and an error
	// if there was an issue subscribing to the channel.
	claims, err := Subscribe[*internal.ClaimRequest](
		ctx, c.bus, getClaimRequestChannel(serviceName, clientID), c.channelSize,
	)
	// The code is checking if the error variable is not nil. If it is not nil, it closes the responses
	// object and returns nil and the error.
	if err != nil {
		_ = responses.Close()
		return nil, err
	}

	// The below code is declaring a variable named `streams` of type `Subscription` which is a generic
	// type that takes a parameter of `*internal.Stream`. The `*` indicates that it is a pointer to a
	// `Stream` object. The variable is initialized to an empty array of pointers to `Stream` objects.
	var streams Subscription[*internal.Stream]
	// The below code is checking if the `enableStreams` flag is set to true. If it is true, it subscribes
	// to a stream using the `Subscribe` function with the given context, bus, channel name, and channel
	// size. If there is an error during subscription, it closes the `responses` and `claims` channels and
	// returns the error. If the `enableStreams` flag is false, it sets the `streams` variable to an empty
	// subscription of type `nilSubscription[*internal.Stream]`.
	if c.enableStreams {
		streams, err = Subscribe[*internal.Stream](
			ctx, c.bus, getStreamChannel(serviceName, clientID), c.channelSize,
		)
		if err != nil {
			_ = responses.Close()
			_ = claims.Close()
			return nil, err
		}
	} else {
		streams = nilSubscription[*internal.Stream]{}
	}

	// The below code is defining a goroutine that listens to multiple channels using a select statement.
	// It listens to the `closed` channel to close other channels and return when it receives a signal to
	// close. It also listens to the `claims`, `responses`, and `streams` channels to receive messages and
	// forward them to the appropriate channels based on their request or stream IDs. The code is likely
	// part of a larger program that involves communication between different components or services.
	go func() {
		for {
			select {
			case <-c.closed:
				_ = claims.Close()
				_ = responses.Close()
				_ = streams.Close()
				return

			case claim := <-claims.Channel():
				c.mu.RLock()
				claimChan, ok := c.claimRequests[claim.RequestId]
				c.mu.RUnlock()
				if ok {
					claimChan <- claim
				}

			case res := <-responses.Channel():
				c.mu.RLock()
				resChan, ok := c.responseChannels[res.RequestId]
				c.mu.RUnlock()
				if ok {
					resChan <- res
				}

			case msg := <-streams.Channel():
				c.mu.RLock()
				streamChan, ok := c.streamChannels[msg.StreamId]
				c.mu.RUnlock()
				if ok {
					streamChan <- msg
				}

			}
		}
	}()

	return c, nil
}

// This function creates a new RPC client with streams.
func NewRPCClientWithStreams(serviceName, clientID string, bus MessageBus, opts ...ClientOption) (*RPCClient, error) {
	opts = append([]ClientOption{withStreams()}, opts...)
	return NewRPCClient(serviceName, clientID, bus, opts...)
}

// The RPCClient type represents a client for making remote procedure calls.
// @property {clientOpts}  - - `clientOpts`: a struct containing options for the RPC client
// @property {MessageBus} bus - The `bus` property is a `MessageBus` object that represents the message
// bus used by the RPC client to communicate with the RPC server. The message bus is responsible for
// transmitting messages between the client and server.
// @property {string} serviceName - The name of the remote service that the RPC client is communicating
// with.
// @property {string} id - The `id` property is a unique identifier for the RPC client instance. It is
// used to distinguish between multiple clients connected to the same message bus and to route
// responses and streams back to the correct client.
// @property mu - The `mu` property is a `sync.RWMutex` type, which is a mutual exclusion lock that is
// used to protect shared resources from concurrent access. It allows multiple readers or a single
// writer to access the resource at the same time, but not both a reader and a writer simultaneously.
// In
// @property claimRequests - A map that stores channels for each claim request made by the client. The
// key is a string representing the claim request ID and the value is a channel that will receive the
// response for that claim request.
// @property responseChannels - The `responseChannels` property is a map that stores channels for
// receiving responses from the RPC server. The keys of the map are unique identifiers for each RPC
// call, and the values are channels that will receive the response when it is available. This allows
// the client to make asynchronous RPC calls and receive the
// @property streamChannels - `streamChannels` is a map that stores channels for receiving streaming
// responses from the RPC server. Each channel is associated with a unique stream ID. When a streaming
// response is received from the server, it is sent to the corresponding channel for processing by the
// client. This allows the client to handle streaming responses
// @property closed - The `closed` property is a channel used to signal when the RPC client has been
// closed and all associated resources have been cleaned up. It is of type `chan struct{}` which is a
// channel that does not carry any data, but is used for synchronization purposes. When the channel is
// closed,
type RPCClient struct {
	clientOpts

	bus              MessageBus
	serviceName      string
	id               string
	mu               sync.RWMutex
	claimRequests    map[string]chan *internal.ClaimRequest
	responseChannels map[string]chan *internal.Response
	streamChannels   map[string]chan *internal.Stream
	closed           chan struct{}
}

// The below code is defining a method called `Close()` for a type `RPCClient`. This method checks if
// the `closed` channel of the `RPCClient` instance is already closed or not. If it is not closed, then
// it closes the channel by calling the `close()` function. The `select` statement is used to avoid
// blocking if the channel is already closed.
func (c *RPCClient) Close() {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
}

// This is a function for making a single RPC request with various options and hooks, and handling the
// response and errors.
func RequestSingle[ResponseType proto.Message](
	ctx context.Context,
	c *RPCClient,
	rpc string,
	topic []string,
	requireClaim bool,
	request proto.Message,
	opts ...RequestOption,
) (response ResponseType, err error) {

	// The below code is creating a variable `info` of type `RPCInfo` and initializing its fields
	// `Service`, `Method`, and `Topic` with values `c.serviceName`, `rpc`, and `topic` respectively. It
	// is likely part of a larger program that involves remote procedure calls (RPCs) and messaging
	// topics.
	info := RPCInfo{
		Service: c.serviceName,
		Method:  rpc,
		Topic:   topic,
	}

	// response hooks
	defer func() {
		for _, hook := range c.responseHooks {
			hook(ctx, request, info, response, err)
		}
	}()

	// request hooks
	for _, hook := range c.requestHooks {
		hook(ctx, request, info)
	}

	call := func(ctx context.Context, request proto.Message, opts ...RequestOption) (response proto.Message, err error) {
		o := getRequestOpts(c.clientOpts, opts...)

		b, a, err := serializePayload(request)
		if err != nil {
			err = NewError(MalformedRequest, err)
			return
		}

		requestID := newRequestID()
		now := time.Now()
		req := &internal.Request{
			RequestId:  requestID,
			ClientId:   c.id,
			SentAt:     now.UnixNano(),
			Expiry:     now.Add(o.timeout).UnixNano(),
			Multi:      false,
			Request:    a,
			RawRequest: b,
			Metadata:   OutgoingContextMetadata(ctx),
		}

		claimChan := make(chan *internal.ClaimRequest, c.channelSize)
		resChan := make(chan *internal.Response, 1)

		c.mu.Lock()
		c.claimRequests[requestID] = claimChan
		c.responseChannels[requestID] = resChan
		c.mu.Unlock()

		defer func() {
			c.mu.Lock()
			delete(c.claimRequests, requestID)
			delete(c.responseChannels, requestID)
			c.mu.Unlock()
		}()

		if err = c.bus.Publish(ctx, getRPCChannel(c.serviceName, rpc, topic), req); err != nil {
			err = NewError(Internal, err)
			return
		}

		ctx, cancel := context.WithTimeout(ctx, o.timeout)
		defer cancel()

		if requireClaim {
			serverID, err := selectServer(ctx, claimChan, resChan, o.selectionOpts)
			if err != nil {
				return nil, err
			}
			if err = c.bus.Publish(ctx, getClaimResponseChannel(c.serviceName, rpc, topic), &internal.ClaimResponse{
				RequestId: requestID,
				ServerId:  serverID,
			}); err != nil {
				err = NewError(Internal, err)
				return nil, err
			}
		}

		select {
		case res := <-resChan:
			if res.Error != "" {
				err = newErrorFromResponse(res.Code, res.Error)
			} else {
				response, err = deserializePayload[ResponseType](res.RawResponse, res.Response)
				if err != nil {
					err = NewError(MalformedResponse, err)
				}
			}

		case <-ctx.Done():
			err = ctx.Err()
			if errors.Is(err, context.Canceled) {
				err = ErrRequestCanceled
			} else if errors.Is(err, context.DeadlineExceeded) {
				err = ErrRequestTimedOut
			}
		}

		return
	}

	res, err := chainClientInterceptors[RPCInterceptor](c.rpcInterceptors, info, call)(ctx, request, opts...)
	if res != nil {
		response, _ = res.(ResponseType)
	}
	return
}

func selectServer(
	ctx context.Context,
	claimChan chan *internal.ClaimRequest,
	resChan chan *internal.Response,
	opts SelectionOpts,
) (string, error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if opts.AffinityTimeout > 0 {
		time.AfterFunc(opts.AffinityTimeout, cancel)
	}

	serverID := ""
	best := float32(0)
	shorted := false
	claims := 0
	var resErr error

	for {
		select {
		case <-ctx.Done():
			if best > 0 {
				return serverID, nil
			}
			if resErr != nil {
				return "", resErr
			}
			if claims == 0 {
				return "", ErrNoResponse
			}
			return "", NewErrorf(Unavailable, "no servers available (received %d responses)", claims)

		case claim := <-claimChan:
			claims++
			if (opts.MinimumAffinity > 0 && claim.Affinity >= opts.MinimumAffinity && claim.Affinity > best) ||
				(opts.MinimumAffinity <= 0 && claim.Affinity > best) {
				if opts.AcceptFirstAvailable {
					return claim.ServerId, nil
				}

				serverID = claim.ServerId
				best = claim.Affinity

				if opts.ShortCircuitTimeout > 0 && !shorted {
					shorted = true
					time.AfterFunc(opts.ShortCircuitTimeout, cancel)
				}
			}

		case res := <-resChan:
			// will only happen with malformed requests
			if res.Error != "" {
				resErr = NewErrorf(ErrorCode(res.Code), res.Error)
			}
		}
	}
}

type Response[ResponseType proto.Message] struct {
	Result ResponseType
	Err    error
}

func RequestMulti[ResponseType proto.Message](
	ctx context.Context,
	c *RPCClient,
	rpc string,
	topic []string,
	request proto.Message,
	opts ...RequestOption,
) (rChan <-chan *Response[ResponseType], err error) {

	info := RPCInfo{
		Service: c.serviceName,
		Method:  rpc,
		Topic:   topic,
		Multi:   true,
	}

	responseChannel := make(chan *Response[ResponseType], c.channelSize)
	call := &multiRPC[ResponseType]{
		c:         c,
		requestID: newRequestID(),
		resChan:   responseChannel,
		info:      info,
	}
	call.interceptor = chainClientInterceptors[MultiRPCInterceptor](c.multiRPCInterceptors, info, &multiRPCInterceptorRoot[ResponseType]{call})

	// request hooks
	for _, hook := range c.requestHooks {
		hook(ctx, request, info)
	}

	if err = call.interceptor.Send(ctx, request, opts...); err != nil {
		for _, hook := range c.responseHooks {
			hook(ctx, request, info, nil, err)
		}
		return
	}

	return responseChannel, nil
}

func Join[ResponseType proto.Message](
	ctx context.Context,
	c *RPCClient,
	rpc string,
	topic []string,
) (Subscription[ResponseType], error) {
	sub, err := Subscribe[ResponseType](ctx, c.bus, getRPCChannel(c.serviceName, rpc, topic), c.channelSize)
	if err != nil {
		return nil, NewError(Internal, err)
	}
	return sub, nil
}

func JoinQueue[ResponseType proto.Message](
	ctx context.Context,
	c *RPCClient,
	rpc string,
	topic []string,
) (Subscription[ResponseType], error) {
	sub, err := SubscribeQueue[ResponseType](ctx, c.bus, getRPCChannel(c.serviceName, rpc, topic), c.channelSize)
	if err != nil {
		return nil, NewError(Internal, err)
	}
	return sub, nil
}

func OpenStream[SendType, RecvType proto.Message](
	ctx context.Context,
	c *RPCClient,
	rpc string,
	topic []string,
	requireClaim bool,
	opts ...RequestOption,
) (ClientStream[SendType, RecvType], error) {

	o := getRequestOpts(c.clientOpts, opts...)
	info := RPCInfo{
		Service: c.serviceName,
		Method:  rpc,
		Topic:   topic,
	}

	streamID := newStreamID()
	requestID := newRequestID()
	now := time.Now()
	req := &internal.Stream{
		StreamId:  streamID,
		RequestId: requestID,
		SentAt:    now.UnixNano(),
		Expiry:    now.Add(o.timeout).UnixNano(),
		Body: &internal.Stream_Open{
			Open: &internal.StreamOpen{
				NodeId:   c.id,
				Metadata: OutgoingContextMetadata(ctx),
			},
		},
	}

	claimChan := make(chan *internal.ClaimRequest, c.channelSize)
	recvChan := make(chan *internal.Stream, c.channelSize)

	c.mu.Lock()
	c.claimRequests[requestID] = claimChan
	c.streamChannels[streamID] = recvChan
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.claimRequests, requestID)
		c.mu.Unlock()
	}()

	octx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()

	if err := c.bus.Publish(octx, getStreamServerChannel(c.serviceName, rpc, topic), req); err != nil {
		return nil, NewError(Internal, err)
	}

	if requireClaim {
		serverID, err := selectServer(octx, claimChan, nil, o.selectionOpts)
		if err != nil {
			return nil, err
		}
		if err = c.bus.Publish(octx, getClaimResponseChannel(c.serviceName, rpc, topic), &internal.ClaimResponse{
			RequestId: requestID,
			ServerId:  serverID,
		}); err != nil {
			return nil, NewError(Internal, err)
		}
	}

	ackChan := make(chan struct{})

	stream := &streamImpl[SendType, RecvType]{
		streamOpts: streamOpts{
			timeout: c.timeout,
		},
		adapter: &clientStream{
			c:    c,
			info: info,
		},
		recvChan: make(chan RecvType, c.channelSize),
		streamID: streamID,
		acks:     map[string]chan struct{}{requestID: ackChan},
	}
	stream.ctx, stream.cancelCtx = context.WithCancel(ctx)
	stream.interceptor = chainClientInterceptors[StreamInterceptor](c.streamInterceptors, info, &streamInterceptorRoot[SendType, RecvType]{stream})

	go runClientStream(c, stream, recvChan)

	select {
	case <-ackChan:
		return stream, nil

	case <-octx.Done():
		err := octx.Err()
		if errors.Is(err, context.Canceled) {
			err = ErrRequestCanceled
		} else if errors.Is(err, context.DeadlineExceeded) {
			err = ErrRequestTimedOut
		}
		_ = stream.Close(err)
		return nil, err
	}
}

func runClientStream[SendType, RecvType proto.Message](
	c *RPCClient,
	s *streamImpl[SendType, RecvType],
	recvChan chan *internal.Stream,
) {
	for {
		select {
		case <-s.ctx.Done():
			_ = s.Close(s.ctx.Err())
			return

		case <-c.closed:
			_ = s.Close(nil)
			return

		case is := <-recvChan:
			if time.Now().UnixNano() < is.Expiry {
				if err := s.handleStream(is); err != nil {
					logger.Error(err, "failed to handle request", "requestID", is.RequestId)
				}
			}
		}
	}
}
