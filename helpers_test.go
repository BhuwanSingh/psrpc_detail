package psrpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/livekit/psrpc/internal"
)

func TestSerialization(t *testing.T) {
	msg := &internal.Request{
		RequestId: "reid",
		ClientId:  "clid",
		SentAt:    time.Now().UnixNano(),
		Multi:     true,
	}

	b, err := serialize(msg)
	require.NoError(t, err)

	m, err := deserialize(b)
	require.NoError(t, err)

	require.Equal(t, m.(*internal.Request).RequestId, msg.RequestId)
	require.Equal(t, m.(*internal.Request).ClientId, msg.ClientId)
	require.Equal(t, m.(*internal.Request).SentAt, msg.SentAt)
	require.Equal(t, m.(*internal.Request).Multi, msg.Multi)
}

func TestRawSerialization(t *testing.T) {
	msg := &internal.Request{
		RequestId: "reid",
		ClientId:  "clid",
		SentAt:    time.Now().UnixNano(),
		Multi:     true,
	}

	b, a, err := serializePayload(msg)
	require.NoError(t, err)

	msg0, err := deserializePayload[*internal.Request](b, nil)
	require.NoError(t, err)
	require.True(t, proto.Equal(msg, msg0), "expected deserialized raw payload to match source")

	msg1, err := deserializePayload[*internal.Request](nil, a)
	require.NoError(t, err)
	require.True(t, proto.Equal(msg, msg1), "expected deserialized anypb payload to match source")

	msg2, err := deserializePayload[*internal.Request](b, a)
	require.NoError(t, err)
	require.True(t, proto.Equal(msg, msg2), "expected deserialized mixed payload to match source")

	msg3, err := a.UnmarshalNew()
	require.NoError(t, err)
	require.True(t, proto.Equal(msg, msg3), "expected anypb decoded payload to match source")
}

func TestChannelFormatters(t *testing.T) {
	require.Equal(t, "foo|bar|a|b|c|REQ", getRPCChannel("foo", "bar", []string{"a", "b", "c"}))
	require.Equal(t, "foo|bar|REQ", getRPCChannel("foo", "bar", nil))
	require.Equal(t, "foo|a|b|c", getHandlerKey("foo", []string{"a", "b", "c"}))
	require.Equal(t, "foo|bar|RES", getResponseChannel("foo", "bar"))
	require.Equal(t, "foo|bar|CLAIM", getClaimRequestChannel("foo", "bar"))
	require.Equal(t, "foo|bar|a|b|c|RCLAIM", getClaimResponseChannel("foo", "bar", []string{"a", "b", "c"}))
	require.Equal(t, "foo|bar|RCLAIM", getClaimResponseChannel("foo", "bar", nil))
	require.Equal(t, "foo|bar|STR", getStreamChannel("foo", "bar"))
	require.Equal(t, "foo|bar|a|b|c|STR", getStreamServerChannel("foo", "bar", []string{"a", "b", "c"}))
	require.Equal(t, "foo|bar|STR", getStreamServerChannel("foo", "bar", nil))

	require.Equal(t, "U+0001f680_u+00c9|U+0001f6f0_bar|u+8f6fu+4ef6|END", formatChannel("🚀_É", "🛰_bar", []string{"软件"}, "END"))
}
