package config

import (
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		args []string
		want Config
		fail bool
	}{
		{name: "defaults", want: Config{Addr: "127.0.0.1:8080", Debug: false, MaxBodyBytes: 524288, AOFEnabled: true, AOFPath: "data/appendonly.aof", AOFMaxBytes: 512 << 20, AOFRewriteMinBytes: 4 << 20, MaxDataBytes: 256 << 20, AOFSyncInterval: time.Second}},
		{name: "env", env: map[string]string{"MINI_REDIS_ADDR": "127.0.0.1:9090", "MINI_REDIS_DEBUG": "true", "MINI_REDIS_MAX_BODY_BYTES": "64"}, want: Config{Addr: "127.0.0.1:9090", Debug: true, MaxBodyBytes: 64, AOFEnabled: true, AOFPath: "data/appendonly.aof", AOFMaxBytes: 512 << 20, AOFRewriteMinBytes: 4 << 20, MaxDataBytes: 256 << 20, AOFSyncInterval: time.Second}},
		{name: "flag_false", env: map[string]string{"MINI_REDIS_DEBUG": "true"}, args: []string{"-debug=false"}, want: Config{Addr: "127.0.0.1:8080", Debug: false, MaxBodyBytes: 524288, AOFEnabled: true, AOFPath: "data/appendonly.aof", AOFMaxBytes: 512 << 20, AOFRewriteMinBytes: 4 << 20, MaxDataBytes: 256 << 20, AOFSyncInterval: time.Second}},
		{name: "flag_overrides_bad_env", env: map[string]string{"MINI_REDIS_DEBUG": "invalid"}, args: []string{"-debug"}, want: Config{Addr: "127.0.0.1:8080", Debug: true, MaxBodyBytes: 524288, AOFEnabled: true, AOFPath: "data/appendonly.aof", AOFMaxBytes: 512 << 20, AOFRewriteMinBytes: 4 << 20, MaxDataBytes: 256 << 20, AOFSyncInterval: time.Second}},
		{name: "aof_disabled", env: map[string]string{"MINI_REDIS_AOF": "false"}, want: Config{Addr: "127.0.0.1:8080", Debug: false, MaxBodyBytes: 524288, AOFEnabled: false, AOFPath: "data/appendonly.aof", AOFMaxBytes: 512 << 20, AOFRewriteMinBytes: 4 << 20, MaxDataBytes: 256 << 20, AOFSyncInterval: time.Second}},
		{name: "aof_limit_follows_data_limit", env: map[string]string{"MINI_REDIS_MAX_DATA_BYTES": "1073741824"}, want: Config{Addr: "127.0.0.1:8080", MaxBodyBytes: 524288, AOFEnabled: true, AOFPath: "data/appendonly.aof", AOFMaxBytes: 2 << 30, AOFRewriteMinBytes: 4 << 20, MaxDataBytes: 1 << 30, AOFSyncInterval: time.Second}},
		{name: "small_data_limit_keeps_aof_floor", env: map[string]string{"MINI_REDIS_MAX_DATA_BYTES": "1048576"}, want: Config{Addr: "127.0.0.1:8080", MaxBodyBytes: 524288, AOFEnabled: true, AOFPath: "data/appendonly.aof", AOFMaxBytes: 256 << 20, AOFRewriteMinBytes: 4 << 20, MaxDataBytes: 1 << 20, AOFSyncInterval: time.Second}},
		{name: "explicit_aof_limit_is_kept", env: map[string]string{"MINI_REDIS_AOF_MAX_BYTES": "268435456"}, want: Config{Addr: "127.0.0.1:8080", MaxBodyBytes: 524288, AOFEnabled: true, AOFPath: "data/appendonly.aof", AOFMaxBytes: 256 << 20, AOFRewriteMinBytes: 4 << 20, MaxDataBytes: 256 << 20, AOFSyncInterval: time.Second}},
		{name: "sync_interval", env: map[string]string{"MINI_REDIS_AOF_SYNC_INTERVAL_MS": "250"}, want: Config{Addr: "127.0.0.1:8080", MaxBodyBytes: 524288, AOFEnabled: true, AOFPath: "data/appendonly.aof", AOFMaxBytes: 512 << 20, AOFRewriteMinBytes: 4 << 20, MaxDataBytes: 256 << 20, AOFSyncInterval: 250 * time.Millisecond}},
		{name: "sync_interval_too_short", env: map[string]string{"MINI_REDIS_AOF_SYNC_INTERVAL_MS": "9"}, fail: true},
		{name: "sync_interval_too_long", env: map[string]string{"MINI_REDIS_AOF_SYNC_INTERVAL_MS": "60001"}, fail: true},
		{name: "sync_interval_not_integer", env: map[string]string{"MINI_REDIS_AOF_SYNC_INTERVAL_MS": "1s"}, fail: true},
		{name: "bad_aof_limit", env: map[string]string{"MINI_REDIS_AOF_MAX_BYTES": "0"}, fail: true},
		{name: "bad_rewrite_threshold", env: map[string]string{"MINI_REDIS_AOF_MAX_BYTES": "64"}, fail: true},
		{name: "bad_aof", env: map[string]string{"MINI_REDIS_AOF": "bad"}, fail: true},
		{name: "empty_aof_path", env: map[string]string{"MINI_REDIS_AOF_PATH": ""}, fail: true},
		{name: "bad_debug", env: map[string]string{"MINI_REDIS_DEBUG": "bad"}, fail: true},
		{name: "bad_size", env: map[string]string{"MINI_REDIS_MAX_BODY_BYTES": "0"}, fail: true},
		{name: "non_integer_size", env: map[string]string{"MINI_REDIS_MAX_BODY_BYTES": "1.5"}, fail: true},
		{name: "bad_addr", env: map[string]string{"MINI_REDIS_ADDR": "localhost"}, fail: true},
		{name: "bad_port", env: map[string]string{"MINI_REDIS_ADDR": "localhost:65536"}, fail: true},
		{name: "unknown_flag", args: []string{"-unknown"}, fail: true},
		{name: "positional_arg", args: []string{"unused"}, fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Load(tc.args, func(key string) (string, bool) { v, ok := tc.env[key]; return v, ok })
			if tc.fail {
				if err == nil {
					t.Fatal("expected invalid config")
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got=%+v error=%v want=%+v", got, err, tc.want)
			}
		})
	}
}

func TestDeploymentSettings(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		fail bool
	}{
		{"public_requires_token", map[string]string{"MINI_REDIS_ADDR": "0.0.0.0:8080"}, true},
		{"public_with_token", map[string]string{"MINI_REDIS_ADDR": "0.0.0.0:8080", "MINI_REDIS_AUTH_TOKEN": "0123456789abcdef"}, false},
		{"explicit_override", map[string]string{"MINI_REDIS_ADDR": "0.0.0.0:8080", "MINI_REDIS_ALLOW_UNAUTHENTICATED": "true"}, false},
		{"short_token", map[string]string{"MINI_REDIS_AUTH_TOKEN": "short"}, true},
		{"bad_override", map[string]string{"MINI_REDIS_ALLOW_UNAUTHENTICATED": "bad"}, true},
		{"bad_limit", map[string]string{"MINI_REDIS_MAX_DATA_BYTES": "0"}, true},
		{"limit", map[string]string{"MINI_REDIS_MAX_DATA_BYTES": "1024"}, false},
		{"ipv6_loopback", map[string]string{"MINI_REDIS_ADDR": "[::1]:8080"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Load(nil, func(k string) (string, bool) { v, ok := tc.env[k]; return v, ok })
			if (err != nil) != tc.fail {
				t.Fatalf("unexpected config error: %v", err)
			}
			if tc.name == "limit" && cfg.MaxDataBytes != 1024 {
				t.Fatal("limit not applied")
			}
		})
	}
}

func TestAOFLimitHeadroom(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  Config
		want bool
	}{
		{"default", Config{AOFEnabled: true, AOFMaxBytes: 512 << 20, MaxDataBytes: 256 << 20}, false},
		{"equal_limits", Config{AOFEnabled: true, AOFMaxBytes: 256 << 20, MaxDataBytes: 256 << 20}, true},
		{"memory_mode", Config{AOFEnabled: false, AOFMaxBytes: 256 << 20, MaxDataBytes: 256 << 20}, false},
	} {
		if got := tc.cfg.AOFLimitTooSmall(); got != tc.want {
			t.Fatalf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
	if got := DefaultAOFMaxBytes(1 << 62); got <= 0 {
		t.Fatalf("overflowed default AOF limit: %d", got)
	}
}
