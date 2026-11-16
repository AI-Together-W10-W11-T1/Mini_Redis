// Command cachebench compares product lookups with and without mini-Redis as a
// cache-aside layer. It runs every condition combination, keeps per-repeat
// results and writes CSV/Markdown summaries. See docs/benchmark.md.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type options struct {
	scenarios, caches, aofs []string
	delays, concs           []int
	repeats                 int
	products, valueBytes    int
	warmup, duration        time.Duration
	writeDuration           time.Duration
	coldKeys                int
	stampedeKeys, perKey    int
	timeout, ttl            time.Duration
	out                     string
	bin, redisURL, token    string
	auth                    bool
	seed                    int64
	label                   string
	dryRun                  bool
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func intList(name, s string) ([]int, error) {
	var out []int
	for _, p := range splitList(s) {
		v, err := strconv.Atoi(p)
		if err != nil || v < 0 {
			return nil, fmt.Errorf("-%s: %q is not a non-negative integer", name, p)
		}
		out = append(out, v)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("-%s must not be empty", name)
	}
	return out, nil
}

func oneOf(name string, vals, allowed []string) error {
	if len(vals) == 0 {
		return fmt.Errorf("-%s must not be empty", name)
	}
	for _, v := range vals {
		ok := false
		for _, a := range allowed {
			ok = ok || v == a
		}
		if !ok {
			return fmt.Errorf("-%s: %q (allowed: %s)", name, v, strings.Join(allowed, ","))
		}
	}
	return nil
}

func parseOptions(args []string, stderr io.Writer) (options, error) {
	var o options
	fs := flag.NewFlagSet("cachebench", flag.ContinueOnError)
	fs.SetOutput(stderr)
	scen := fs.String("scenarios", "warm,cold,stampede,write", "warm (prefilled, time-boxed), cold (first lookups, request-count bound), stampede (duplicate concurrent misses), write (sustained PUTs for AOF cost)")
	cache := fs.String("cache", "off,on", "cache modes to compare")
	aof := fs.String("aof", "off,on", "mini-Redis AOF settings")
	delays := fs.String("delays", "0,20", "artificial origin delays in ms")
	concs := fs.String("concurrency", "1,20", "concurrent clients")
	fs.IntVar(&o.repeats, "repeats", 5, "repeats per condition; the order of OFF / AOF off / AOF on rotates every repeat")
	fs.IntVar(&o.products, "products", 100, "warm and write scenario key space (uniform)")
	fs.IntVar(&o.valueBytes, "value-bytes", 1024, "approximate product JSON size")
	fs.DurationVar(&o.warmup, "warmup", 3*time.Second, "warm/write: unrecorded warm-up")
	fs.DurationVar(&o.duration, "duration", 30*time.Second, "warm: measured duration")
	fs.DurationVar(&o.writeDuration, "write-duration", 10*time.Second, "write: measured duration (spans this many 1s AOF sync periods)")
	fs.IntVar(&o.coldKeys, "cold-keys", 1000, "cold: distinct keys per repeat, each looked up once")
	fs.IntVar(&o.stampedeKeys, "stampede-keys", 500, "stampede: distinct keys per repeat")
	fs.IntVar(&o.perKey, "stampede-requests-per-key", 2, "stampede: back-to-back requests per key (>= 2)")
	fs.DurationVar(&o.timeout, "timeout", 5*time.Second, "timeout of one logical lookup (cache-aside flow)")
	fs.DurationVar(&o.ttl, "ttl", 10*time.Minute, "TTL of cached products")
	fs.StringVar(&o.out, "out", "bench-results", "output parent directory")
	fs.StringVar(&o.bin, "miniredis-bin", "", "mini-Redis binary (default: go build ./cmd/miniredis)")
	fs.StringVar(&o.redisURL, "redis-url", "", "use an already running mini-Redis instead of spawning (then -aof must be the one value that server uses)")
	fs.StringVar(&o.token, "token", "", "Bearer token for -redis-url (default: $MINI_REDIS_AUTH_TOKEN)")
	fs.BoolVar(&o.auth, "auth", true, "spawn mode: enable Bearer auth on mini-Redis, as in production")
	fs.Int64Var(&o.seed, "seed", 1, "random seed for key selection")
	fs.StringVar(&o.label, "label", "", "free-form note stored with the results")
	fs.BoolVar(&o.dryRun, "dry-run", false, "print the plan and estimated time, then exit")
	quick := fs.Bool("quick", false, "smoke-test preset: warmup 1s, duration 5s, write-duration 3s, repeats 2, cold-keys 200, stampede-keys 100")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() != 0 {
		return o, errors.New("unexpected positional arguments")
	}
	if *quick {
		set := map[string]bool{}
		fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
		if !set["warmup"] {
			o.warmup = time.Second
		}
		if !set["duration"] {
			o.duration = 5 * time.Second
		}
		if !set["write-duration"] {
			o.writeDuration = 3 * time.Second
		}
		if !set["repeats"] {
			o.repeats = 2
		}
		if !set["cold-keys"] {
			o.coldKeys = 200
		}
		if !set["stampede-keys"] {
			o.stampedeKeys = 100
		}
	}
	o.scenarios, o.caches, o.aofs = splitList(*scen), splitList(*cache), splitList(*aof)
	if err := oneOf("scenarios", o.scenarios, []string{"warm", "cold", "stampede", "write"}); err != nil {
		return o, err
	}
	if err := oneOf("cache", o.caches, []string{"off", "on"}); err != nil {
		return o, err
	}
	if err := oneOf("aof", o.aofs, []string{"off", "on"}); err != nil {
		return o, err
	}
	var err error
	if o.delays, err = intList("delays", *delays); err != nil {
		return o, err
	}
	if o.concs, err = intList("concurrency", *concs); err != nil {
		return o, err
	}
	for _, c := range o.concs {
		if c < 1 {
			return o, errors.New("-concurrency values must be >= 1")
		}
	}
	if o.redisURL != "" {
		if len(o.aofs) != 1 {
			return o, errors.New("with -redis-url, pass the single -aof value that server uses (e.g. -aof on)")
		}
		o.redisURL = strings.TrimRight(o.redisURL, "/")
		if o.token == "" {
			o.token = os.Getenv("MINI_REDIS_AUTH_TOKEN")
		}
	}
	if o.repeats < 1 || o.products < 1 || o.coldKeys < 1 || o.stampedeKeys < 1 || o.perKey < 2 || o.valueBytes < 200 || o.valueBytes > 60000 {
		return o, errors.New("repeats/products/cold-keys/stampede-keys must be >= 1, stampede-requests-per-key >= 2 and value-bytes 200..60000")
	}
	if o.duration <= 0 || o.writeDuration <= 0 || o.warmup < 0 || o.timeout <= 0 {
		return o, errors.New("duration, write-duration and timeout must be > 0, warmup >= 0")
	}
	if minTTL := o.warmup + o.duration + o.writeDuration + time.Minute; o.ttl < minTTL {
		o.ttl = minTTL
	}
	return o, nil
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// variant is one thing measured inside a repeat.
type variant struct{ cache, aof string }

func (v variant) String() string {
	if v.cache == "off" {
		return "off"
	}
	return "on/aof-" + v.aof
}

// variantsFor lists what a scenario compares. Cache OFF never touches
// mini-Redis, so it appears once; the write scenario has no cache-OFF form.
func variantsFor(o options, scen string) []variant {
	var vs []variant
	for _, c := range o.caches {
		switch {
		case c == "off" && scen != "write":
			vs = append(vs, variant{"off", "-"})
		case c == "on":
			for _, a := range o.aofs {
				vs = append(vs, variant{"on", a})
			}
		}
	}
	return vs
}

func delaysFor(o options, scen string) []int {
	if scen == "write" {
		return []int{0} // the origin is not involved
	}
	return o.delays
}

// rotate returns vs shifted by n, so every variant takes every position in
// turn across repeats and time-dependent drift is spread evenly.
func rotate(vs []variant, n int) []variant {
	out := make([]variant, len(vs))
	for i := range vs {
		out[i] = vs[(i+n)%len(vs)]
	}
	return out
}

// estimate is a rough wall-clock figure used for the plan printout.
func estimate(o options) (runs int, d time.Duration) {
	for _, s := range o.scenarios {
		vs := variantsFor(o, s)
		for _, dl := range delaysFor(o, s) {
			for _, c := range o.concs {
				var per time.Duration
				switch s {
				case "warm":
					per = o.warmup + o.duration
				case "write":
					per = o.warmup + o.writeDuration
				default:
					reqs := o.coldKeys
					if s == "stampede" {
						reqs = o.stampedeKeys * o.perKey
					}
					per = time.Duration(reqs/c+1) * (time.Duration(dl)*time.Millisecond + 400*time.Microsecond)
				}
				runs += len(vs) * o.repeats
				d += time.Duration(len(vs)*o.repeats) * (per + 1500*time.Millisecond)
			}
		}
	}
	return runs, d
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "cachebench:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	o, err := parseOptions(args, stderr)
	if err != nil {
		return err
	}
	root, err := findModuleRoot()
	if err != nil {
		return err
	}
	nRuns, eta := estimate(o)
	fmt.Fprintf(stdout, "조건: scenarios=%v cache=%v aof=%v delays=%vms concurrency=%v repeats=%d\n", o.scenarios, o.caches, o.aofs, o.delays, o.concs, o.repeats)
	fmt.Fprintf(stdout, "총 %d회 실행, 예상 소요 약 %s\n", nRuns, eta.Round(time.Second))
	if o.dryRun {
		return nil
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// The random suffix keeps two sessions started in the same second from
	// sharing an output directory or, on an external server, a key prefix.
	sessionID := time.Now().Format("20060102-150405") + "-" + randomHex(3)
	outDir := filepath.Join(o.out, sessionID)
	if err := ensureDir(outDir); err != nil {
		return err
	}
	work, err := os.MkdirTemp("", "cachebench-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(work)

	mode := "spawn"
	if o.redisURL != "" {
		mode = "external"
	}
	env := collectEnv(root, mode, (mode == "spawn" && o.auth) || o.token != "", o.label)
	bin := o.bin
	if contains(o.caches, "on") {
		switch {
		case mode == "external":
			env.Topology = "CLI + origin on this host; mini-Redis at " + o.redisURL + " (location not verified by the tool)"
			env.ServerBuild = "external server: code, build and debug setting are user-declared, not verified"
			env.ServerConfig = "user-declared (only the AOF mode is checked against /health)"
			got, err := waitHealthy(o.redisURL, 5*time.Second)
			if err != nil {
				return err
			}
			want := map[string]string{"on": "aof", "off": "disabled"}[o.aofs[0]]
			if got != want {
				return fmt.Errorf("-aof %s was declared, but %s/health reports persistence=%q", o.aofs[0], o.redisURL, got)
			}
		case bin == "":
			fmt.Fprintln(stdout, "mini-Redis 빌드 중 (go build ./cmd/miniredis)...")
			if bin, err = buildServer(root, work); err != nil {
				return err
			}
			env.ServerBuild = "built by cachebench from this source tree (source_hash)"
		default:
			env.ServerBuild = "user-supplied binary " + bin + " (source_hash does not describe it; see server_binary)"
		}
		if mode == "spawn" {
			info, err := inspectBinary(bin)
			if err != nil {
				return err
			}
			env.ServerBinary = &info
			env.ServerConfig = "set by cachebench: debug off, fresh process and empty AOF per run"
		}
	}
	plan := planInfo{o.scenarios, o.caches, o.aofs, o.delays, o.concs, o.repeats, o.products, o.valueBytes,
		o.warmup.String(), o.duration.String(), o.writeDuration.String(), o.coldKeys, o.stampedeKeys, o.perKey, o.timeout.String(), o.seed}
	if err := writeJSON(filepath.Join(outDir, "meta.json"), map[string]any{"env": env, "plan": plan, "session": sessionID}); err != nil {
		return err
	}

	catSize := o.products
	for _, n := range []int{o.coldKeys, o.stampedeKeys} {
		if n > catSize {
			catSize = n
		}
	}
	cat, err := newCatalog(catSize, o.valueBytes)
	if err != nil {
		return err
	}
	org, err := startOrigin(cat)
	if err != nil {
		return err
	}
	defer org.close()

	runsCSV, err := newCSV(filepath.Join(outDir, "runs.csv"), append(append([]string{}, runColumns...), envColumns...))
	if err != nil {
		return err
	}
	defer runsCSV.close()

	var allRuns []runRow
	var summaries []summaryRow
	done := 0
	began := time.Now()

	// runOne measures one variant once. A spawned server lives only for this
	// call: stop is deferred right after the start, so no error path can leave
	// the process or its scratch AOF behind.
	runOne := func(scen string, v variant, delay, conc, rep int, runID string) (runRow, error) {
		spec := runSpec{
			scenario: scen, cacheOn: v.cache == "on", concurrency: conc, keys: o.products, perKey: 1,
			warmup: o.warmup, duration: o.duration, ttlMs: o.ttl.Milliseconds(),
			prefix: "bench:" + runID + ":", seed: o.seed + int64(rep)*1000003,
		}
		switch scen {
		case "cold":
			spec.keys = o.coldKeys
		case "stampede":
			spec.keys, spec.perKey = o.stampedeKeys, o.perKey
		case "write":
			spec.duration = o.writeDuration
		}
		// A fresh client per run: every run starts with zero open connections.
		httpClient, dials := newHTTPClient(conc)
		defer httpClient.CloseIdleConnections()
		cl := &client{http: httpClient, cat: cat, originURL: org.url, timeout: o.timeout, dials: dials}
		var tgt *target
		if v.cache == "on" {
			if mode == "spawn" {
				tgt, err = spawnServer(bin, work, v.aof == "on", o.auth, filepath.Join(outDir, "server-aof-"+v.aof+".log"))
				if err != nil {
					return runRow{}, err
				}
				defer tgt.stop()
			} else {
				tgt = &target{url: o.redisURL, token: o.token}
			}
			cl.redisURL, cl.token, spec.aofPath = tgt.url, tgt.token, tgt.aofPath
		}
		started := time.Now()
		raw := execute(ctx, cl, spec)
		row := buildRow(spec, raw)
		row.RunID, row.Started, row.Cache, row.AOF, row.DelayMs, row.Repeat = runID, started, v.cache, v.aof, delay, rep
		if tgt != nil && tgt.stop != nil {
			ex := tgt.stop()
			row.ServerExit, row.ServerCPUS, row.ServerRSSMB = ex.Status, ex.CPU.Seconds(), float64(ex.MaxRSS)/(1<<20)
		}
		return row, nil
	}

	for _, scen := range o.scenarios {
		vs := variantsFor(o, scen)
		if len(vs) == 0 {
			continue
		}
		for _, delay := range delaysFor(o, scen) {
			org.setDelay(time.Duration(delay) * time.Millisecond)
			for _, conc := range o.concs {
				var block []runRow
				for rep := 1; rep <= o.repeats && ctx.Err() == nil; rep++ {
					for pos, v := range rotate(vs, rep-1) {
						if ctx.Err() != nil {
							break
						}
						runID := fmt.Sprintf("%s-%04d", sessionID, done+1)
						row, err := runOne(scen, v, delay, conc, rep, runID)
						if err != nil {
							return err
						}
						if ctx.Err() != nil {
							break // a cut-short run would skew the medians
						}
						row.Order = pos + 1
						if err := runsCSV.write(runRecord(row, env)); err != nil {
							return err
						}
						done++
						block = append(block, row)
						left := time.Duration(float64(time.Since(began)) / float64(done) * float64(nRuns-done))
						note := ""
						if !row.Valid {
							note = "  무효: " + row.Invalid
						}
						fmt.Fprintf(stdout, "[%d/%d] %-8s cache=%-3s aof=%-3s delay=%2dms conc=%-3d rep=%d  rps=%9.1f p50=%8.3fms p99=%8.3fms hit=%s fail=%d  (남은 시간 약 %s)%s\n",
							done, nRuns, scen, v.cache, v.aof, delay, conc, rep, row.SuccessRPS, row.P50, row.P99,
							fmtF(row.HitRatio*100, 1), row.Errors+row.Timeouts+row.Validation, left.Round(time.Second), note)
					}
				}
				groups := map[groupKey][]runRow{}
				var order []groupKey
				for _, r := range block {
					k := r.key()
					if _, ok := groups[k]; !ok {
						order = append(order, k)
					}
					groups[k] = append(groups[k], r)
				}
				for _, k := range order {
					summaries = append(summaries, summarize(groups[k]))
				}
				allRuns = append(allRuns, block...)
			}
		}
	}

	interrupted := ctx.Err() != nil
	compare(summaries, allRuns)
	sortSummaries(summaries)
	sumCSV, err := newCSV(filepath.Join(outDir, "summary.csv"), append(append([]string{}, summaryColumns...), envColumns...))
	if err != nil {
		return err
	}
	for _, s := range summaries {
		if err := sumCSV.write(summaryRecord(s, env)); err != nil {
			return err
		}
	}
	if err := sumCSV.close(); err != nil {
		return err
	}
	if err := writeReport(filepath.Join(outDir, "report.md"), env, plan, summaries, allRuns, interrupted); err != nil {
		return err
	}
	writeTable(stdout, summaries)
	if ws := warnings(summaries, allRuns, env); len(ws) > 0 {
		fmt.Fprintln(stdout, "\n점검 필요:")
		for _, s := range ws {
			fmt.Fprintln(stdout, " -", s)
		}
	}
	fmt.Fprintf(stdout, "\n결과: %s (report.md, summary.csv, runs.csv, meta.json)\n", outDir)
	if interrupted {
		return errors.New("interrupted; finished runs were saved")
	}
	return nil
}
