package config

import (
	"flag"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

type Config struct {
	Addr         string
	Debug        bool
	MaxBodyBytes int64
}

const (
	ReadHeaderTimeout = 3 * time.Second
	ReadTimeout       = 5 * time.Second
	WriteTimeout      = 5 * time.Second
	IdleTimeout       = 60 * time.Second
	ShutdownTimeout   = 5 * time.Second
)

// Load accepts the environment reader explicitly so tests need no shared env.
func Load(args []string, lookup func(string) (string, bool)) (Config, error) {
	cfg := Config{Addr: "127.0.0.1:8080", MaxBodyBytes: 524288}
	if value, ok := lookup("MINI_REDIS_ADDR"); ok {
		cfg.Addr = value
	}
	if value, ok := lookup("MINI_REDIS_MAX_BODY_BYTES"); ok {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed <= 0 {
			return Config{}, fmt.Errorf("MINI_REDIS_MAX_BODY_BYTES must be a positive integer")
		}
		cfg.MaxBodyBytes = parsed
	}
	flags := flag.NewFlagSet("miniredis", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.BoolVar(&cfg.Debug, "debug", false, "enable debug logs")
	if err := flags.Parse(args); err != nil {
		return Config{}, err
	}
	if flags.NArg() != 0 {
		return Config{}, fmt.Errorf("unexpected positional arguments")
	}
	explicitDebug := false
	flags.Visit(func(f *flag.Flag) { explicitDebug = explicitDebug || f.Name == "debug" })
	if !explicitDebug {
		if value, ok := lookup("MINI_REDIS_DEBUG"); ok {
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return Config{}, fmt.Errorf("MINI_REDIS_DEBUG must be true/false")
			}
			cfg.Debug = parsed
		}
	}
	host, port, err := net.SplitHostPort(cfg.Addr)
	if err != nil || host == "" {
		return Config{}, fmt.Errorf("MINI_REDIS_ADDR must be host:port")
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 {
		return Config{}, fmt.Errorf("MINI_REDIS_ADDR port must be 1..65535")
	}
	return cfg, nil
}
