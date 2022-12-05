package psrpc

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/anypb"

	"github.com/livekit/psrpc/internal"
)

type rpcServer struct {
	MessageBus
	rpcOpts

	serviceName string
	id          string
	mu          sync.RWMutex
	handlers    map[string]*handler
	claims      map[string]chan *internal.ClaimResponse
	closed      chan struct{}
}

type handler struct {
	handlerFunc  HandlerFunc
	affinityFunc AffinityFunc
	sub          Subscription
}

func NewRPCServer(serviceName, serverID string, bus MessageBus, opts ...RPCOption) (RPCServer, error) {
	s := &rpcServer{
		MessageBus:  bus,
		rpcOpts:     getRPCOpts(opts...),
		serviceName: serviceName,
		id:          serverID,
		handlers:    make(map[string]*handler),
		claims:      make(map[string]chan *internal.ClaimResponse),
		closed:      make(chan struct{}),
	}

	claims, err := s.Subscribe(context.Background(), getClaimResponseChannel(serviceName))
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			select {
			case <-s.closed:
				_ = claims.Close()
				return

			case p := <-claims.Channel():
				claim := p.(*internal.ClaimResponse)

				s.mu.RLock()
				claimChan, ok := s.claims[claim.RequestId]
				s.mu.RUnlock()
				if ok {
					claimChan <- claim
				}
			}
		}
	}()

	return s, nil
}

func (s *rpcServer) RegisterHandler(rpc string, handlerFunc HandlerFunc, opts ...HandlerOption) error {
	sub, err := s.Subscribe(context.Background(), getRequestChannel(s.serviceName, rpc))
	if err != nil {
		return err
	}

	h := &handler{
		handlerFunc: handlerFunc,
		sub:         sub,
	}
	for _, opt := range opts {
		opt(h)
	}

	s.mu.Lock()
	// close previous handler if exists
	if err = s.closeHandlerLocked(rpc); err != nil {
		s.mu.Unlock()
		return err
	}
	s.handlers[rpc] = h
	s.mu.Unlock()

	reqChan := sub.Channel()
	go func() {
		for {
			select {
			case <-s.closed:
				_ = sub.Close()
				return

			case p := <-reqChan:
				go func(req *internal.Request) {
					err := s.handleRequest(rpc, req)
					if err != nil {
						logger.Error(err, "failed to handle request", "requestID", req.RequestId)
					}
				}(p.(*internal.Request))
			}
		}
	}()

	return nil
}

func (s *rpcServer) DeregisterHandler(rpc string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closeHandlerLocked(rpc)
}

func (s *rpcServer) closeHandlerLocked(rpc string) error {
	h, ok := s.handlers[rpc]
	if ok {
		delete(s.handlers, rpc)
		return h.sub.Close()
	}
	return nil
}

func (s *rpcServer) handleRequest(rpc string, req *internal.Request) error {
	s.mu.RLock()
	h, ok := s.handlers[rpc]
	s.mu.RUnlock()
	if !ok {
		return errors.New("handler not found")
	}

	ctx := context.Background()
	request, err := req.Request.UnmarshalNew()
	if err != nil {
		return err
	}

	if !req.Multi {
		affinity := float32(1)
		if h.affinityFunc != nil {
			affinity = h.affinityFunc(request)
		}

		claimed, err := s.claimRequest(ctx, req, affinity)
		if err != nil {
			return err
		} else if !claimed {
			return nil
		}
	}

	res := &internal.Response{
		RequestId: req.RequestId,
		ServerId:  s.id,
		SentAt:    time.Now().UnixNano(),
	}

	// call handler function
	response, err := h.handlerFunc(ctx, request)
	if err != nil {
		res.Error = err.Error()
	} else {
		v, err := anypb.New(response)
		if err != nil {
			return err
		}
		res.Response = v
	}

	return s.Publish(ctx, getResponseChannel(s.serviceName, req.ClientId), res)
}

func (s *rpcServer) claimRequest(ctx context.Context, request *internal.Request, affinity float32) (bool, error) {
	claimResponseChan := make(chan *internal.ClaimResponse, 1)

	s.mu.Lock()
	s.claims[request.RequestId] = claimResponseChan
	s.mu.Unlock()

	err := s.Publish(ctx, getClaimRequestChannel(s.serviceName, request.ClientId), &internal.ClaimRequest{
		RequestId: request.RequestId,
		ServerId:  s.id,
		Affinity:  affinity,
	})
	if err != nil {
		return false, err
	}

	defer func() {
		s.mu.Lock()
		delete(s.claims, request.RequestId)
		s.mu.Unlock()
	}()

	select {
	case claim := <-claimResponseChan:
		if claim.ServerId == s.id {
			return true, nil
		} else {
			return false, nil
		}

	case <-time.After(s.timeout):
		return false, errors.New("no response from server")
	}
}

func (s *rpcServer) Close() {
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}
}
