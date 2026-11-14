package config

import "testing"

func TestLoad(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		args []string
		want Config
		fail bool
	}{
		{name: "defaults", want: Config{"127.0.0.1:8080", false, 524288}},
		{name: "env", env: map[string]string{"MINI_REDIS_ADDR": "127.0.0.1:9090", "MINI_REDIS_DEBUG": "true", "MINI_REDIS_MAX_BODY_BYTES": "64"}, want: Config{"127.0.0.1:9090", true, 64}},
		{name: "flag_false", env: map[string]string{"MINI_REDIS_DEBUG": "true"}, args: []string{"-debug=false"}, want: Config{"127.0.0.1:8080", false, 524288}},
		{name: "flag_overrides_bad_env", env: map[string]string{"MINI_REDIS_DEBUG": "invalid"}, args: []string{"-debug"}, want: Config{"127.0.0.1:8080", true, 524288}},
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
