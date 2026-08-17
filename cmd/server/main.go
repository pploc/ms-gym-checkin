package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pploc/common-go/grpc/interceptor"
	"github.com/pploc/common-go/grpc/middleware"
	commonkafka "github.com/pploc/common-go/kafka"
	"github.com/pploc/common-go/observability"
	checkinv1 "github.com/pploc/proto-go/checkin/v1"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"

	grpcadapter "github.com/pploc/ms-gym-checkin/internal/adapter/grpc"
	kafkaadapter "github.com/pploc/ms-gym-checkin/internal/adapter/kafka"
	memberadapter "github.com/pploc/ms-gym-checkin/internal/adapter/member"
	plansadapter "github.com/pploc/ms-gym-checkin/internal/adapter/plans"
	vaultadapter "github.com/pploc/ms-gym-checkin/internal/adapter/vault"
	yugabyteadapter "github.com/pploc/ms-gym-checkin/internal/adapter/yugabyte"
	"github.com/pploc/ms-gym-checkin/internal/config"
	"github.com/pploc/ms-gym-checkin/internal/usecase"
)

func main() {
	if err := run(); err != nil {
		log.Print("server stopped unexpectedly")
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	store, err := yugabyteadapter.New(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("yugabyte: %w", err)
	}
	defer store.Close()
	member, err := memberadapter.New(cfg.Member)
	if err != nil {
		return fmt.Errorf("member client: %w", err)
	}
	defer member.Close()
	plans, err := plansadapter.New(cfg.Plans)
	if err != nil {
		return fmt.Errorf("plans client: %w", err)
	}
	defer plans.Close()
	vault, err := vaultadapter.New(cfg.VaultAddress, cfg.VaultAuth, cfg.VaultTransitMount, cfg.VaultKeyReference)
	if err != nil {
		return fmt.Errorf("vault: %w", err)
	}
	service := usecase.NewService(store, member, plans, vault, usecase.SystemClock{}, usecase.UUIDGenerator{})
	registry, err := middleware.NewRegistry(grpcadapter.MethodRules()...)
	if err != nil {
		return fmt.Errorf("method registry: %w", err)
	}
	metrics, err := observability.NewMetrics(otel.Meter(cfg.ServiceName))
	if err != nil {
		return fmt.Errorf("metrics: %w", err)
	}
	validator, err := interceptor.NewValidator()
	if err != nil {
		return fmt.Errorf("protovalidate: %w", err)
	}
	serverTLS, err := serverCredentials(cfg)
	if err != nil {
		return err
	}
	serverOpts := interceptor.ServerOptions(interceptor.AuthOptions{PublicMethods: grpcadapter.PublicMethods()}, registry, metrics, validator)
	serverOpts = append(serverOpts, grpc.Creds(serverTLS))
	grpcServer := grpc.NewServer(serverOpts...)
	checkinv1.RegisterCheckInServiceServer(grpcServer, grpcadapter.NewHandler(service))
	if cfg.GRPCReflection {
		reflection.Register(grpcServer)
	}
	listener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}
	defer listener.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	go runOutboxRelay(ctx, cfg, store)
	httpServer := &http.Server{Addr: cfg.HTTPAddr, Handler: healthHandler(service, cfg.ReadinessTimeout), ReadHeaderTimeout: 5 * time.Second}
	serveErr := make(chan error, 2)
	go func() {
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			serveErr <- fmt.Errorf("serve grpc: %w", err)
		}
	}()
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- fmt.Errorf("serve HTTP: %w", err)
		}
	}()
	var serveFailure error
	select {
	case <-ctx.Done():
	case serveFailure = <-serveErr:
		cancel()
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelShutdown()
	if err := httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
		serveFailure = errors.Join(serveFailure, fmt.Errorf("shutdown HTTP: %w", err))
	}
	stopped := make(chan struct{})
	go func() { grpcServer.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-shutdownCtx.Done():
		grpcServer.Stop()
	}
	return serveFailure
}

func healthHandler(service *usecase.Service, timeout time.Duration) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		if err := service.Ready(ctx); err != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
func serverCredentials(cfg config.Config) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(cfg.GRPCServerCert, cfg.GRPCServerKey)
	if err != nil {
		return nil, err
	}
	pem, err := os.ReadFile(cfg.GRPCClientCA)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("invalid Check-in client CA")
	}
	return credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, VerifyPeerCertificate: grpcadapter.VerifyGeneratedGateway, MinVersion: tls.VersionTLS12}), nil
}
func newProducer(cfg config.Config) (*commonkafka.FranzProducer, func() error, error) {
	registry, err := commonkafka.NewConfluentProtobufRegistry(commonkafka.RegistryConfig{URL: cfg.SchemaRegistryURL})
	if err != nil {
		return nil, nil, err
	}
	producer, err := commonkafka.NewFranzProducer(commonkafka.TransportConfig{Brokers: strings.Split(cfg.KafkaBrokers, ","), PublishTimeout: 5 * time.Second}, registry)
	if err != nil {
		_ = registry.Close()
		return nil, nil, err
	}
	return producer, func() error {
		producer.Close()
		return registry.Close()
	}, nil
}
func runOutboxRelay(ctx context.Context, cfg config.Config, store *yugabyteadapter.Store) {
	for {
		if ctx.Err() != nil {
			return
		}
		producer, closeProducer, err := newProducer(cfg)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
				continue
			}
		}
		relay := kafkaadapter.NewRelay(store, producer, cfg.ServiceName)
		relay.Run(ctx, cfg.RelayInterval)
		_ = closeProducer()
	}
}
