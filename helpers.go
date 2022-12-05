package psrpc

import (
	"fmt"
	"strings"

	"github.com/lithammer/shortuuid/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func newRequestID() string {
	return "req_" + shortuuid.New()[:12]
}

func getRequestChannel(serviceName, rpc string) string {
	return strings.ToUpper(fmt.Sprintf("%s_%s", serviceName, rpc))
}

func getResponseChannel(serviceName, clientID string) string {
	return strings.ToUpper(fmt.Sprintf("%s_res_%s", serviceName, clientID))
}

func getClaimRequestChannel(serviceName, clientID string) string {
	return strings.ToUpper(fmt.Sprintf("%s_claim_req_%s", serviceName, clientID))
}

func getClaimResponseChannel(serviceName string) string {
	return strings.ToUpper(fmt.Sprintf("%s_claim_res", serviceName))
}

func serialize(msg proto.Message) ([]byte, error) {
	a, err := anypb.New(msg)
	if err != nil {
		return nil, err
	}

	b, err := proto.Marshal(a)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func deserialize(b []byte) (proto.Message, error) {
	a := &anypb.Any{}
	err := proto.Unmarshal(b, a)
	if err != nil {
		return nil, err
	}

	return a.UnmarshalNew()
}
