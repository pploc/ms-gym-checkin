package unit

import (
	"context"
	"net"
	"testing"

	"github.com/pploc/common-go/auth"
	commonerrors "github.com/pploc/common-go/errors"
	"github.com/pploc/common-go/grpc/interceptor"
	"github.com/pploc/common-go/grpc/middleware"
	checkinv1 "github.com/pploc/proto-go/checkin/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	grpcadapter "github.com/pploc/ms-gym-checkin/internal/adapter/grpc"
)

func TestGivenGatewayCustomerClaims_WhenCallingCustomerHistory_ThenHandlerUsesTrustedSubject(t *testing.T) {
	// Given
	listener, server := checkInBufconn(t)
	defer server.Stop()
	conn := bufconnConnection(t, listener)
	defer conn.Close()
	request := &checkinv1.GetMyCheckInHistoryRequest{}
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(auth.HeaderUserID, "trusted-user", auth.HeaderUserRole, "CUSTOMER"))

	// When
	response := &checkinv1.GetMyCheckInHistoryResponse{}
	err := conn.Invoke(ctx, checkinv1.CheckInService_GetMyCheckInHistory_FullMethodName, request, response)

	// Then
	if err != nil || response.GetTotal() != 1 {
		t.Fatal("trusted customer request was not accepted")
	}
}

func TestGivenMissingClaims_WhenCallingCustomerHistory_ThenReturnsAuthenticationTrailer(t *testing.T) {
	// Given
	listener, server := checkInBufconn(t)
	defer server.Stop()
	conn := bufconnConnection(t, listener)
	defer conn.Close()
	var trailers metadata.MD

	// When
	err := conn.Invoke(context.Background(), checkinv1.CheckInService_GetMyCheckInHistory_FullMethodName, &checkinv1.GetMyCheckInHistoryRequest{}, &checkinv1.GetMyCheckInHistoryResponse{}, grpc.Trailer(&trailers))

	// Then
	if status.Code(err) != codes.Unauthenticated || trailers.Get(commonerrors.TrailerErrorCode)[0] != "AUTHENTICATION_REQUIRED" {
		t.Fatal("missing claims did not fail closed")
	}
}

func TestGivenCustomerClaims_WhenCallingAdminDailyCount_ThenReturnsForbiddenTrailer(t *testing.T) {
	// Given
	listener, server := checkInBufconn(t)
	defer server.Stop()
	conn := bufconnConnection(t, listener)
	defer conn.Close()
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(auth.HeaderUserID, "user", auth.HeaderUserRole, "CUSTOMER"))
	var trailers metadata.MD

	// When
	err := conn.Invoke(ctx, checkinv1.CheckInService_GetDailyCount_FullMethodName, &checkinv1.GetDailyCountRequest{}, &checkinv1.GetDailyCountResponse{}, grpc.Trailer(&trailers))

	// Then
	if status.Code(err) != codes.PermissionDenied || len(trailers.Get(commonerrors.TrailerErrorCode)) != 1 || trailers.Get(commonerrors.TrailerErrorCode)[0] != "ROLE_FORBIDDEN" {
		t.Fatal("customer reached super-admin method")
	}
}

func checkInBufconn(t *testing.T) (*bufconn.Listener, *grpc.Server) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	registry, err := middleware.NewRegistry(grpcadapter.MethodRules()...)
	if err != nil {
		t.Fatal("create method registry failed")
	}
	validator, err := interceptor.NewValidator()
	if err != nil {
		t.Fatal("create request validator failed")
	}
	server := grpc.NewServer(interceptor.ServerOptions(interceptor.AuthOptions{Claims: auth.DefaultOptions(), PublicMethods: grpcadapter.PublicMethods()}, registry, nil, validator)...)
	checkinv1.RegisterCheckInServiceServer(server, interceptorTestServer{})
	go func() { _ = server.Serve(listener) }()
	return listener, server
}

func bufconnConnection(t *testing.T, listener *bufconn.Listener) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///checkin", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal("create bufconn client failed")
	}
	return conn
}

type interceptorTestServer struct {
	checkinv1.UnimplementedCheckInServiceServer
}

func (interceptorTestServer) GetMyCheckInHistory(ctx context.Context, _ *checkinv1.GetMyCheckInHistoryRequest) (*checkinv1.GetMyCheckInHistoryResponse, error) {
	claims, ok := auth.FromContext(ctx)
	if !ok || claims.UserID != "trusted-user" {
		return nil, commonerrors.New(commonerrors.CategoryUnauthorized, "AUTHENTICATION_REQUIRED", "authentication is required")
	}
	return &checkinv1.GetMyCheckInHistoryResponse{Total: 1}, nil
}
