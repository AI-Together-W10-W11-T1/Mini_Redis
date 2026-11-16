package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/AI-Together-W10-W11-T1/Mini_Redis/internal/config"
	"github.com/AI-Together-W10-W11-T1/Mini_Redis/internal/httpapi"
	"github.com/AI-Together-W10-W11-T1/Mini_Redis/internal/store"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) == 2 && os.Args[1] == "-version" {
		fmt.Println(version)
		return nil
	}
	cfg, err := config.Load(os.Args[1:], os.LookupEnv)
	if err != nil {
		return err
	}
	level := slog.LevelInfo
	if cfg.Debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	if cfg.AOFLimitTooSmall() {
		logger.Warn("AOF_LIMIT_BELOW_DATA_LIMIT", "aof_max_bytes", cfg.AOFMaxBytes, "max_data_bytes", cfg.MaxDataBytes,
			"hint", "writes may fail with PERSISTENCE_UNAVAILABLE before the data limit; use at least twice the data limit")
	}
	var s *store.Store
	if cfg.AOFEnabled {
		opts := store.DefaultAOFOptions(cfg.AOFPath)
		opts.MaxBytes = cfg.AOFMaxBytes
		opts.RewriteMinBytes = cfg.AOFRewriteMinBytes
		opts.SyncInterval = cfg.AOFSyncInterval
		// 미동기화 기록이 Sync 주기 세 번을 넘게 밀리면 변경을 거부한다(기본 주기에서는 3초).
		opts.MaxUnsyncedAge = 3 * cfg.AOFSyncInterval
		s, err = store.OpenPersistentWithLimit(nil, logger, opts, cfg.MaxDataBytes)
		if err != nil {
			return fmt.Errorf("open AOF: %w", err)
		}
	} else {
		s = store.NewWithLimit(nil, logger, cfg.MaxDataBytes)
	}
	server := &http.Server{
		Addr: cfg.Addr, Handler: httpapi.NewHandlerWithOptions(s, logger, httpapi.Options{MaxBodyBytes: cfg.MaxBodyBytes, AuthToken: cfg.AuthToken}),
		ReadHeaderTimeout: config.ReadHeaderTimeout,
		ReadTimeout:       config.ReadTimeout, WriteTimeout: config.WriteTimeout,
		IdleTimeout: config.IdleTimeout,
	}
	listener, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		_ = s.Close()
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	finished := make(chan error, 1)
	go func() { finished <- server.Serve(listener) }()
	logger.Info("START", "version", version, "addr", listener.Addr().String(), "debug", cfg.Debug, "max_body_bytes", cfg.MaxBodyBytes)
	select {
	case err := <-finished:
		closeErr := s.Close()
		if !errors.Is(err, http.ErrServerClosed) {
			return errors.Join(err, closeErr)
		}
		return closeErr
	case <-ctx.Done():
		stop()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout)
		defer cancel()
		closed := make(chan error, 1)
		go func() {
			shutdownErr := server.Shutdown(shutdownCtx)
			if shutdownErr != nil {
				_ = server.Close()
			}
			closed <- errors.Join(shutdownErr, s.Close())
		}()
		select {
		case err := <-closed:
			if err != nil {
				return err
			}
		case <-shutdownCtx.Done():
			_ = server.Close()
			return fmt.Errorf("shutdown deadline: %w", shutdownCtx.Err())
		}
		if err := <-finished; !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		logger.Info("STOP")
		return nil
	}
}
