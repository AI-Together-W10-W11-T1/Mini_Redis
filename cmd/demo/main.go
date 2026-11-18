// Command demo runs the two helper services of the caching demo: a slow
// origin API and a consuming app that caches origin responses in mini-Redis.
// mini-Redis itself is started separately (cmd/miniredis).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type options struct {
	originAddr    string
	appAddr       string
	originURL     string
	redisURL      string
	token         string
	delay         time.Duration
	ttlMillis     int64
	products      int
	timeout       time.Duration
	cacheFallback bool
	cacheTimeout  time.Duration
}

func parseOptions(args []string, lookup func(string) (string, bool)) (options, error) {
	var o options
	flags := flag.NewFlagSet("demo", flag.ContinueOnError)
	flags.StringVar(&o.originAddr, "origin-addr", "127.0.0.1:8081", "origin API listen address")
	flags.StringVar(&o.appAddr, "app-addr", "127.0.0.1:8082", "consuming app listen address")
	flags.StringVar(&o.originURL, "origin-url", "", "use an already running origin instead of starting one")
	flags.StringVar(&o.redisURL, "redis-url", "http://127.0.0.1:8080", "mini-Redis base URL")
	flags.DurationVar(&o.delay, "origin-delay", 100*time.Millisecond, "artificial origin response delay")
	flags.Int64Var(&o.ttlMillis, "cache-ttl-ms", 60_000, "TTL of cached products in milliseconds")
	flags.IntVar(&o.products, "products", 100, "number of seeded products (ids 1..N)")
	flags.DurationVar(&o.timeout, "timeout", 3*time.Second, "timeout of calls to the origin (also caps cache calls)")
	flags.BoolVar(&o.cacheFallback, "cache-fallback", false, "return origin responses when cache reads or fills fail")
	flags.DurationVar(&o.cacheTimeout, "cache-timeout", 200*time.Millisecond, "timeout of each mini-Redis call when cache-fallback is enabled")
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if flags.NArg() != 0 {
		return options{}, fmt.Errorf("unexpected positional arguments")
	}
	if o.delay < 0 || o.ttlMillis <= 0 || o.products < 0 || o.timeout <= 0 || o.cacheTimeout <= 0 {
		return options{}, fmt.Errorf("origin-delay must be >= 0; cache-ttl-ms, timeout and cache-timeout must be positive; products must be >= 0")
	}
	o.redisURL = strings.TrimRight(o.redisURL, "/")
	o.originURL = strings.TrimRight(o.originURL, "/")
	// The token is read from the environment so it never appears in a process list.
	o.token, _ = lookup("MINI_REDIS_AUTH_TOKEN")
	return o, nil
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	o, err := parseOptions(args, os.LookupEnv)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var servers []*http.Server
	failed := make(chan error, 2)
	serve := func(name, addr string, handler http.Handler) (string, error) {
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			return "", fmt.Errorf("%s: %w", name, err)
		}
		server := &http.Server{Handler: handler, ReadHeaderTimeout: 3 * time.Second}
		servers = append(servers, server)
		go func() {
			if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
				failed <- fmt.Errorf("%s: %w", name, err)
			}
		}()
		return "http://" + listener.Addr().String(), nil
	}
	shutdown := func() {
		deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, server := range servers {
			_ = server.Shutdown(deadline)
		}
	}
	defer shutdown()

	originURL := o.originURL
	if originURL == "" {
		if originURL, err = serve("origin", o.originAddr, newOrigin(o.products, o.delay).handler()); err != nil {
			return err
		}
		fmt.Printf("origin  %s  (delay %s, products 1..%d)\n", originURL, o.delay, o.products)
	}
	consumer := &app{
		client:    &http.Client{Timeout: o.timeout, Transport: &http.Transport{MaxIdleConns: 256, MaxIdleConnsPerHost: 128, IdleConnTimeout: 90 * time.Second}},
		originURL: originURL, redisURL: o.redisURL, token: o.token, ttlMillis: o.ttlMillis,
		cacheFallback: o.cacheFallback, cacheTimeout: o.cacheTimeout, logger: slog.Default(),
	}
	appURL, err := serve("app", o.appAddr, consumer.handler())
	if err != nil {
		return err
	}
	fmt.Printf("app     %s  (cache %s, ttl %dms)\n", appURL, o.redisURL, o.ttlMillis)

	select {
	case err := <-failed:
		return err
	case <-ctx.Done():
		return nil
	}
}
