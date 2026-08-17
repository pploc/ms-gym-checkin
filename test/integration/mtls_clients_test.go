//go:build integration

package integration

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	commonv1 "github.com/pploc/proto-go/common/v1"
	memberv1 "github.com/pploc/proto-go/member/v1"
	plansv1 "github.com/pploc/proto-go/plans/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	memberadapter "github.com/pploc/ms-gym-checkin/internal/adapter/member"
	plansadapter "github.com/pploc/ms-gym-checkin/internal/adapter/plans"
	"github.com/pploc/ms-gym-checkin/internal/config"
)

func TestGivenCheckInMTLSIdentity_WhenCallingMemberAndPlans_ThenClientsMapValidatedResponses(t *testing.T) {
	// Given
	certs := testCertificates(t)
	memberAddress := startMemberServer(t, certs, false)
	plansAddress := startPlansServer(t, certs)
	member, err := memberadapter.New(clientConfig(memberAddress, certs, "localhost"))
	if err != nil {
		t.Fatal("create Member mTLS client failed")
	}
	t.Cleanup(func() { _ = member.Close() })
	plans, err := plansadapter.New(clientConfig(plansAddress, certs, "localhost"))
	if err != nil {
		t.Fatal("create Plans mTLS client failed")
	}
	t.Cleanup(func() { _ = plans.Close() })

	// When
	membership, memberErr := member.ValidateMembership(context.Background(), "user", testGymID)
	gym, plansErr := plans.ValidateCheckInGym(context.Background(), testGymID)

	// Then
	if memberErr != nil || !membership.Valid || membership.Status != "ACTIVE" || membership.MemberID != "member" {
		t.Fatal("Member mTLS validation failed")
	}
	if plansErr != nil || gym.ID != testGymID || gym.Status != "ACTIVE" {
		t.Fatal("Plans mTLS validation failed")
	}
}

func TestGivenRejectedCheckInIdentity_WhenCallingMember_ThenClientMapsUnauthenticated(t *testing.T) {
	// Given
	certs := testCertificates(t)
	address := startMemberServer(t, certs, true)
	client, err := memberadapter.New(clientConfigWithCertificate(address, certs, "localhost", "client-other"))
	if err != nil {
		t.Fatal("create denied Member mTLS client failed")
	}
	t.Cleanup(func() { _ = client.Close() })

	// When
	_, err = client.ValidateMembership(context.Background(), "user", testGymID)

	// Then
	if err == nil || err.Error() != "membership service rejected credentials" {
		t.Fatal("Member client did not map denied workload identity")
	}
}

func rejectNonCheckInWorkload(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if info.FullMethod != memberv1.MemberService_ValidateMembership_FullMethodName {
		return handler(ctx, request)
	}
	peerInfo, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing workload identity")
	}
	tlsInfo, ok := peerInfo.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing workload identity")
	}
	for _, name := range tlsInfo.State.PeerCertificates[0].DNSNames {
		if name == "ms-gym-checkin" {
			return handler(ctx, request)
		}
	}
	return nil, status.Error(codes.Unauthenticated, "denied workload identity")
}

type memberServer struct {
	memberv1.UnimplementedMemberServiceServer
}

func (memberServer) ValidateMembership(context.Context, *memberv1.ValidateMembershipRequest) (*memberv1.ValidateMembershipResponse, error) {
	return &memberv1.ValidateMembershipResponse{Valid: true, Status: commonv1.MembershipStatus_MEMBERSHIP_STATUS_ACTIVE, MemberId: "member"}, nil
}

type plansServer struct {
	plansv1.UnimplementedPlansServiceServer
}

func (plansServer) ValidateCheckInGym(context.Context, *plansv1.ValidateCheckInGymRequest) (*plansv1.ValidateCheckInGymResponse, error) {
	return &plansv1.ValidateCheckInGymResponse{GymId: testGymID, Status: plansv1.GymLocationStatus_GYM_LOCATION_STATUS_ACTIVE}, nil
}

func startMemberServer(t *testing.T, certs string, denied bool) string {
	t.Helper()
	listener := tlsListener(t, certs)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLSConfig(t, certs))))
	if denied {
		server = grpc.NewServer(
			grpc.Creds(credentials.NewTLS(serverTLSConfig(t, certs))),
			grpc.UnaryInterceptor(rejectNonCheckInWorkload),
		)
	}
	memberv1.RegisterMemberServiceServer(server, memberServer{})
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	return listener.Addr().String()
}

func startPlansServer(t *testing.T, certs string) string {
	t.Helper()
	listener := tlsListener(t, certs)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLSConfig(t, certs))))
	plansv1.RegisterPlansServiceServer(server, plansServer{})
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	return listener.Addr().String()
}

func tlsListener(t *testing.T, certs string) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("listen for test TLS server failed")
	}
	return listener
}

func serverTLSConfig(t *testing.T, certs string) *tls.Config {
	t.Helper()
	certificate, err := tls.LoadX509KeyPair(filepath.Join(certs, "server.crt"), filepath.Join(certs, "server.key"))
	if err != nil {
		t.Fatal("load test server certificate failed")
	}
	pem, err := os.ReadFile(filepath.Join(certs, "ca.crt"))
	if err != nil {
		t.Fatal("read test certificate authority failed")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		t.Fatal("invalid test CA")
	}
	return &tls.Config{Certificates: []tls.Certificate{certificate}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12}
}

func clientConfig(address, certs, serverName string) config.MTLSClient {
	return clientConfigWithCertificate(address, certs, serverName, "client-checkin")
}

func clientConfigWithCertificate(address, certs, serverName, certificate string) config.MTLSClient {
	return config.MTLSClient{Address: address, CertFile: filepath.Join(certs, certificate+".crt"), KeyFile: filepath.Join(certs, certificate+".key"), CAFile: filepath.Join(certs, "ca.crt"), ServerName: serverName, Deadline: 2 * time.Second}
}

func testCertificates(t *testing.T) string {
	t.Helper()
	root := filepath.Join("..", "..")
	output, err := exec.Command(filepath.Join(root, "scripts", "generate-test-certs.sh"), filepath.Join(t.TempDir(), "certs")).Output()
	if err != nil {
		t.Fatal("generate test certificates failed")
	}
	return string(output[:len(output)-1])
}
