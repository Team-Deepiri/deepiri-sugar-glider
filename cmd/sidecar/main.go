package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Team-Deepiri/deepiri-platform/platform-services/backend/deepiri-realtime-gateway/synapse-sidecar/internal/config"
	"github.com/Team-Deepiri/deepiri-platform/platform-services/backend/deepiri-realtime-gateway/synapse-sidecar/internal/service"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	svc, err := service.New(cfg)
	if err != nil {
		slog.Error("failed to initialize sidecar", "error", err)
		os.Exit(1)
	}
	defer svc.Close()

	// Support container-native health checks without relying on shell tools in the image.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		if err := svc.CheckReady(context.Background()); err != nil {
			os.Exit(1)
		}
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info(
		"starting synapse sidecar",
		"service",
		cfg.ServiceName,
		"http_listen",
		cfg.ListenAddr,
		"grpc_listen",
		cfg.GRPCListenAddr,
	)
	if err := svc.Run(ctx); err != nil {
		slog.Error("sidecar exited with error", "error", err)
		os.Exit(1)
	}
}
