package member

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	commonerrors "github.com/pploc/common-go/errors"
	"github.com/pploc/ms-gym-checkin/internal/config"
	"github.com/pploc/ms-gym-checkin/internal/usecase/port"
	commonv1 "github.com/pploc/proto-go/common/v1"
	memberv1 "github.com/pploc/proto-go/member/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type Client struct {
	api      memberv1.MemberServiceClient
	conn     *grpc.ClientConn
	deadline time.Duration
}

func New(cfg config.MTLSClient) (*Client, error) {
	creds, err := credentialsFor(cfg)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(cfg.Address, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, err
	}
	return &Client{api: memberv1.NewMemberServiceClient(conn), conn: conn, deadline: cfg.Deadline}, nil
}

func (c *Client) ValidateMembership(ctx context.Context, userID, gymID string) (port.Membership, error) {
	ctx, cancel := context.WithTimeout(ctx, c.deadline)
	defer cancel()
	response, err := c.api.ValidateMembership(ctx, &memberv1.ValidateMembershipRequest{UserId: userID, GymId: gymID})
	if err != nil {
		return port.Membership{}, mapError(err, "MEMBER")
	}
	return port.Membership{MemberID: response.GetMemberId(), Valid: response.GetValid(), Status: membershipStatus(response.GetStatus())}, nil
}
func (c *Client) Close() error { return c.conn.Close() }
func membershipStatus(value commonv1.MembershipStatus) string {
	switch value {
	case commonv1.MembershipStatus_MEMBERSHIP_STATUS_ACTIVE:
		return "ACTIVE"
	case commonv1.MembershipStatus_MEMBERSHIP_STATUS_PAUSED:
		return "PAUSED"
	case commonv1.MembershipStatus_MEMBERSHIP_STATUS_EXPIRED:
		return "EXPIRED"
	default:
		return "NONE"
	}
}

func credentialsFor(cfg config.MTLSClient) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, err
	}
	pem, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("invalid server CA")
	}
	return credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool, ServerName: cfg.ServerName, MinVersion: tls.VersionTLS12}), nil
}
func mapError(err error, dependency string) error {
	switch status.Code(err) {
	case codes.NotFound:
		return commonerrors.New(commonerrors.CategoryNotFound, dependency+"_NOT_FOUND", "membership was not found")
	case codes.InvalidArgument:
		return commonerrors.New(commonerrors.CategoryValidation, dependency+"_INVALID", "membership request is invalid")
	case codes.Unauthenticated:
		return commonerrors.New(commonerrors.CategoryUnauthorized, dependency+"_UNAUTHENTICATED", "membership service rejected credentials")
	case codes.PermissionDenied:
		return commonerrors.New(commonerrors.CategoryForbidden, dependency+"_FORBIDDEN", "membership service denied request")
	default:
		return commonerrors.New(commonerrors.CategoryUnavailable, dependency+"_UNAVAILABLE", "membership service is unavailable")
	}
}
