package plans

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
	plansv1 "github.com/pploc/proto-go/plans/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type Client struct {
	api      plansv1.PlansServiceClient
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
	return &Client{api: plansv1.NewPlansServiceClient(conn), conn: conn, deadline: cfg.Deadline}, nil
}
func (c *Client) ValidateCheckInGym(ctx context.Context, gymID string) (port.Gym, error) {
	ctx, cancel := context.WithTimeout(ctx, c.deadline)
	defer cancel()
	response, err := c.api.ValidateCheckInGym(ctx, &plansv1.ValidateCheckInGymRequest{GymId: gymID})
	if err != nil {
		return port.Gym{}, mapError(err)
	}
	return port.Gym{ID: response.GetGymId(), Status: gymStatus(response.GetStatus())}, nil
}
func (c *Client) Close() error { return c.conn.Close() }
func gymStatus(value plansv1.GymLocationStatus) string {
	if value == plansv1.GymLocationStatus_GYM_LOCATION_STATUS_ACTIVE {
		return "ACTIVE"
	}
	return "UNSPECIFIED"
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
func mapError(err error) error {
	switch status.Code(err) {
	case codes.NotFound:
		return commonerrors.New(commonerrors.CategoryNotFound, "GYM_NOT_FOUND", "gym was not found")
	case codes.InvalidArgument:
		return commonerrors.New(commonerrors.CategoryValidation, "GYM_INVALID", "gym request is invalid")
	case codes.FailedPrecondition:
		return commonerrors.New(commonerrors.CategoryUnprocessable, "GYM_INACTIVE", "gym is not active")
	case codes.Unauthenticated:
		return commonerrors.New(commonerrors.CategoryUnauthorized, "PLANS_UNAUTHENTICATED", "plans service rejected credentials")
	case codes.PermissionDenied:
		return commonerrors.New(commonerrors.CategoryForbidden, "PLANS_FORBIDDEN", "plans service denied request")
	default:
		return commonerrors.New(commonerrors.CategoryUnavailable, "PLANS_UNAVAILABLE", "plans service is unavailable")
	}
}
