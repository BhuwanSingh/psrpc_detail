package psrpc

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/twitchtv/twirp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Error interface {
	error
	Code() ErrorCode

	// convenience methods
	ToHttp() int
	GRPCStatus() *status.Status
}

type ErrorCode string

func NewError(code ErrorCode, err error) Error {
	return &psrpcError{
		error: err,
		code:  code,
	}
}

func NewErrorf(code ErrorCode, msg string, args ...interface{}) Error {
	return &psrpcError{
		error: fmt.Errorf(msg, args...),
		code:  code,
	}
}

const (
	OK ErrorCode = ""

	// Request Canceled by client
	Canceled ErrorCode = "canceled"
	// Could not unmarshal request
	MalformedRequest ErrorCode = "malformed_request"
	// Could not unmarshal result
	MalformedResponse ErrorCode = "malformed_result"
	// Request timed out
	DeadlineExceeded ErrorCode = "deadline_exceeded"
	// Service unavailable due to load and/or affinity constraints
	Unavailable ErrorCode = "unavailable"
	// Unknown (server returned non-psrpc error)
	Unknown ErrorCode = "unknown"

	// Invalid argument in request
	InvalidArgument ErrorCode = "invalid_argument"
	// Entity not found
	NotFound ErrorCode = "not_found"
	// Duplicate creation attempted
	AlreadyExists ErrorCode = "already_exists"
	// Caller does not have required permissions
	PermissionDenied ErrorCode = "permission_denied"
	// Some resource has been exhausted, e.g. memory or quota
	ResourceExhausted ErrorCode = "resource_exhausted"
	// Inconsistent state to carry out request
	FailedPrecondition ErrorCode = "failed_precondition"
	// Request aborted
	Aborted ErrorCode = "aborted"
	// Operation was out of range
	OutOfRange ErrorCode = "out_of_range"
	// Operation is not implemented by the server
	Unimplemented ErrorCode = "unimplemented"
	// Operation failed due to an internal error
	Internal ErrorCode = "internal"
	// Irrecoverable loss or corruption of data
	DataLoss ErrorCode = "data_loss"
	// Similar to PermissionDenied, used when the caller is unidentified
	Unauthenticated ErrorCode = "unauthenticated"
)

type psrpcError struct {
	error
	code ErrorCode
}

func newErrorFromResponse(code, err string) Error {
	if code == "" {
		code = string(Unknown)
	}

	return &psrpcError{
		error: errors.New(err),
		code:  ErrorCode(code),
	}
}

func (e psrpcError) Code() ErrorCode {
	return e.code
}

func (e psrpcError) Unwrap() error {
	return e.error
}

func (e psrpcError) ToHttp() int {
	switch e.code {
	case OK:
		return http.StatusOK
	case Canceled, DeadlineExceeded:
		return http.StatusRequestTimeout
	case Unknown, MalformedResponse, Internal, DataLoss:
		return http.StatusInternalServerError
	case InvalidArgument, MalformedRequest:
		return http.StatusBadRequest
	case NotFound:
		return http.StatusNotFound
	case AlreadyExists, Aborted:
		return http.StatusConflict
	case PermissionDenied:
		return http.StatusForbidden
	case ResourceExhausted:
		return http.StatusTooManyRequests
	case FailedPrecondition:
		return http.StatusPreconditionFailed
	case OutOfRange:
		return http.StatusRequestedRangeNotSatisfiable
	case Unimplemented:
		return http.StatusNotImplemented
	case Unavailable:
		return http.StatusServiceUnavailable
	case Unauthenticated:
		return http.StatusUnauthorized
	default:
		return http.StatusInternalServerError
	}
}

func (e psrpcError) GRPCStatus() *status.Status {
	var c codes.Code
	switch e.code {
	case OK:
		c = codes.OK
	case Canceled:
		c = codes.Canceled
	case Unknown:
		c = codes.Unknown
	case InvalidArgument, MalformedRequest:
		c = codes.InvalidArgument
	case DeadlineExceeded:
		c = codes.DeadlineExceeded
	case NotFound:
		c = codes.NotFound
	case AlreadyExists:
		c = codes.AlreadyExists
	case PermissionDenied:
		c = codes.PermissionDenied
	case ResourceExhausted:
		c = codes.ResourceExhausted
	case FailedPrecondition:
		c = codes.FailedPrecondition
	case Aborted:
		c = codes.Aborted
	case OutOfRange:
		c = codes.OutOfRange
	case Unimplemented:
		c = codes.Unimplemented
	case MalformedResponse, Internal:
		c = codes.Internal
	case Unavailable:
		c = codes.Unavailable
	case DataLoss:
		c = codes.DataLoss
	case Unauthenticated:
		c = codes.Unauthenticated
	default:
		c = codes.Unknown
	}

	return status.New(c, e.Error())
}

func (e psrpcError) toTwirp() twirp.Error {
	var c twirp.ErrorCode
	switch e.code {
	case OK:
		c = twirp.NoError
	case Canceled:
		c = twirp.Canceled
	case Unknown:
		c = twirp.Unknown
	case InvalidArgument:
		c = twirp.InvalidArgument
	case MalformedRequest, MalformedResponse:
		c = twirp.Malformed
	case DeadlineExceeded:
		c = twirp.DeadlineExceeded
	case NotFound:
		c = twirp.NotFound
	case AlreadyExists:
		c = twirp.AlreadyExists
	case PermissionDenied:
		c = twirp.PermissionDenied
	case ResourceExhausted:
		c = twirp.ResourceExhausted
	case FailedPrecondition:
		c = twirp.FailedPrecondition
	case Aborted:
		c = twirp.Aborted
	case OutOfRange:
		c = twirp.OutOfRange
	case Unimplemented:
		c = twirp.Unimplemented
	case Internal:
		c = twirp.Internal
	case Unavailable:
		c = twirp.Unavailable
	case DataLoss:
		c = twirp.DataLoss
	case Unauthenticated:
		c = twirp.Unauthenticated
	default:
		c = twirp.Unknown
	}

	return twirp.NewError(c, e.Error())
}

func (e psrpcError) As(target any) bool {
	switch te := target.(type) {
	case *twirp.Error:
		*te = e.toTwirp()
		return true
	}

	return false
}
