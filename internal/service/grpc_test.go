package service

import (
	"net/http"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGRPCStatusFromHTTPStatus_GatewayTimeout(t *testing.T) {
	t.Parallel()

	err := grpcStatusFromHTTPStatus(http.StatusGatewayTimeout, "timed out")
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("status.FromError() ok = false, want true")
	}
	if st.Code() != codes.DeadlineExceeded {
		t.Fatalf("status code = %s, want %s", st.Code(), codes.DeadlineExceeded)
	}
}
