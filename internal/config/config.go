package config

import (
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"time"
)

type Config struct {
	Addr                 string
	Debug                bool
	MaxBodyBytes         int64
	AOFEnabled           bool
	AOFPath              string
	AOFMaxBytes          int64
	AOFRewriteMinBytes   int64
	AOFSyncInterval      time.Duration
	MaxDataBytes         int64
	AuthToken            string
	AllowUnauthenticated bool
}

const (
	ReadHeaderTimeout = 3 * time.Second
	ReadTimeout       = 5 * time.Second
	WriteTimeout      = 5 * time.Second
	IdleTimeout       = 60 * time.Second
	ShutdownTimeout   = 5 * time.Second
)

// Bounds of MINI_REDIS_AOF_SYNC_INTERVAL_MS.
const (
	MinAOFSyncIntervalMillis = 10
	MaxAOFSyncIntervalMillis = 60_000
)

// AOFHeadroomFactor is the default AOF limit as a multiple of the data limit.
// It is a policy for typical keys and values, not a bound for every input.
const AOFHeadroomFactor = 2

const minDefaultAOFMaxBytes int64 = 256 << 20

// DefaultAOFMaxBytes returns the AOF limit used when none is configured.
func DefaultAOFMaxBytes(maxDataBytes int64) int64 {
	if maxDataBytes > math.MaxInt64/AOFHeadroomFactor {
		return math.MaxInt64
	}
	if limit := maxDataBytes * AOFHeadroomFactor; limit > minDefaultAOFMaxBytes {
		return limit
	}
	return minDefaultAOFMaxBytes
}

// AOFLimitTooSmall reports whether the AOF limit is below the default headroom,
// in which case the file limit is likely to stop writes before the data limit.
func (c Config) AOFLimitTooSmall() bool {
	return c.AOFEnabled && c.AOFMaxBytes/AOFHeadroomFactor < c.MaxDataBytes
}

// Load accepts the environment reader explicitly so tests need no shared env.
func Load(args []string, lookup func(string) (string, bool)) (Config, error) {
	cfg := Config{Addr: "127.0.0.1:8080", MaxBodyBytes: 524288, AOFEnabled: true, AOFPath: "data/appendonly.aof", AOFMaxBytes: 512 << 20, AOFRewriteMinBytes: 4 << 20, MaxDataBytes: 256 << 20, AOFSyncInterval: time.Second}
	for _, item := range []struct {
		name   string
		target *int64
	}{
		{"MINI_REDIS_MAX_DATA_BYTES", &cfg.MaxDataBytes},
		{"MINI_REDIS_AOF_MAX_BYTES", &cfg.AOFMaxBytes},
		{"MINI_REDIS_AOF_REWRITE_MIN_BYTES", &cfg.AOFRewriteMinBytes},
	} {
		if value, ok := lookup(item.name); ok {
			parsed, err := strconv.ParseInt(value, 10, 64)
			if err != nil || parsed <= 0 {
				return Config{}, fmt.Errorf("%s must be a positive integer", item.name)
			}
			*item.target = parsed
		}
	}
	// AOF는 Base64·프레임·삭제 예약 때문에 같은 데이터에 메모리 계산보다 많은 공간을 쓴다.
	// 2배는 일반적인 상품 키·값 조건에서 여유를 확보하기 위한 기본 정책이며, 모든 입력과
	// 누적 기록에서 데이터 상한 도달을 보장하지는 않는다(이스케이프가 많은 키는 더 필요하다).
	// 따로 지정하지 않으면 데이터 상한의 2배로 맞추되 기존 기본값보다 줄이지는 않는다.
	if _, explicit := lookup("MINI_REDIS_AOF_MAX_BYTES"); !explicit {
		cfg.AOFMaxBytes = DefaultAOFMaxBytes(cfg.MaxDataBytes)
	}
	if cfg.AOFRewriteMinBytes >= cfg.AOFMaxBytes || cfg.AOFMaxBytes <= 8 {
		return Config{}, fmt.Errorf("AOF rewrite threshold must be below file size limit")
	}
	if value, ok := lookup("MINI_REDIS_AOF_SYNC_INTERVAL_MS"); ok {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < MinAOFSyncIntervalMillis || parsed > MaxAOFSyncIntervalMillis {
			return Config{}, fmt.Errorf("MINI_REDIS_AOF_SYNC_INTERVAL_MS must be %d..%d", MinAOFSyncIntervalMillis, MaxAOFSyncIntervalMillis)
		}
		cfg.AOFSyncInterval = time.Duration(parsed) * time.Millisecond
	}
	if value, ok := lookup("MINI_REDIS_AOF"); ok {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return Config{}, fmt.Errorf("MINI_REDIS_AOF must be true/false")
		}
		cfg.AOFEnabled = parsed
	}
	if value, ok := lookup("MINI_REDIS_AOF_PATH"); ok {
		if value == "" {
			return Config{}, fmt.Errorf("MINI_REDIS_AOF_PATH must not be empty")
		}
		cfg.AOFPath = value
	}
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
	if value, ok := lookup("MINI_REDIS_AUTH_TOKEN"); ok {
		cfg.AuthToken = value
	}
	if cfg.AuthToken != "" && len(cfg.AuthToken) < 16 {
		return Config{}, fmt.Errorf("MINI_REDIS_AUTH_TOKEN must contain at least 16 bytes")
	}
	if value, ok := lookup("MINI_REDIS_ALLOW_UNAUTHENTICATED"); ok {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return Config{}, fmt.Errorf("MINI_REDIS_ALLOW_UNAUTHENTICATED must be true/false")
		}
		cfg.AllowUnauthenticated = parsed
	}
	host, port, err := net.SplitHostPort(cfg.Addr)
	if err != nil || host == "" {
		return Config{}, fmt.Errorf("MINI_REDIS_ADDR must be host:port")
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 {
		return Config{}, fmt.Errorf("MINI_REDIS_ADDR port must be 1..65535")
	}
	ip := net.ParseIP(host)
	loopback := host == "localhost" || (ip != nil && ip.IsLoopback())
	if !loopback && cfg.AuthToken == "" && !cfg.AllowUnauthenticated {
		return Config{}, fmt.Errorf("non-loopback binding requires MINI_REDIS_AUTH_TOKEN (or explicit MINI_REDIS_ALLOW_UNAUTHENTICATED=true)")
	}
	return cfg, nil
}
