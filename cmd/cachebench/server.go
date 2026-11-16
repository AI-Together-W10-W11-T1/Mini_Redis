package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// serverExit describes how a spawned mini-Redis ended and what it consumed
// over its whole life (start-up, prefill, measurement and cleanup).
type serverExit struct {
	Status   string // "ok", "killed" (did not stop within 10s) or the exit error
	CPU      time.Duration
	MaxRSS   int64
	Recorded bool
}

// target is the mini-Redis instance under test.
type target struct {
	url     string
	token   string
	aofPath string // empty when AOF is off or the server is external
	stop    func() serverExit
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// buildServer compiles ./cmd/miniredis from the module root into dir.
func buildServer(root, dir string) (string, error) {
	bin := filepath.Join(dir, "miniredis")
	if os.PathSeparator == '\\' {
		bin += ".exe"
	}
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/miniredis")
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("go build ./cmd/miniredis: %v\n%s", err, out)
	}
	return bin, nil
}

// binaryInfo identifies the server executable that was actually measured.
type binaryInfo struct {
	SHA256    string            `json:"sha256"`
	GoVersion string            `json:"go_version"`
	Module    string            `json:"module"`
	Settings  map[string]string `json:"build_settings"` // -race, CGO, vcs.revision, vcs.modified, ...
}

func inspectBinary(path string) (binaryInfo, error) {
	var info binaryInfo
	f, err := os.Open(path)
	if err != nil {
		return info, err
	}
	h := sha256.New()
	_, err = io.Copy(h, f)
	f.Close()
	if err != nil {
		return info, err
	}
	info.SHA256 = hex.EncodeToString(h.Sum(nil))
	bi, err := buildinfo.ReadFile(path)
	if err != nil {
		return info, fmt.Errorf("%s is not a Go binary with build info: %w", path, err)
	}
	info.GoVersion, info.Module, info.Settings = bi.GoVersion, bi.Main.Path, map[string]string{}
	for _, s := range bi.Settings {
		info.Settings[s.Key] = s.Value
	}
	return info, nil
}

// spawnServer starts a dedicated mini-Redis process on a free loopback port
// with an empty scratch AOF file, debug logs off and Bearer auth on. The
// user's own MINI_REDIS_* variables are dropped so they cannot change the
// conditions. Every measured run gets its own fresh process, so no run
// inherits another run's keys, AOF size or pending sync/rewrite work.
func spawnServer(bin, workDir string, aofOn bool, auth bool, logPath string) (*target, error) {
	token := ""
	if auth {
		token = randomHex(24)
	}
	env := []string{}
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "MINI_REDIS_") {
			env = append(env, kv)
		}
	}
	// mini-Redis rejects port 0, so reserve a free loopback port and release it.
	addr, err := freeLoopbackAddr()
	if err != nil {
		return nil, err
	}
	aofDir, err := os.MkdirTemp(workDir, "aof-")
	if err != nil {
		return nil, err
	}
	aofPath := filepath.Join(aofDir, "appendonly.aof")
	env = append(env,
		"MINI_REDIS_ADDR="+addr,
		"MINI_REDIS_DEBUG=false",
		fmt.Sprintf("MINI_REDIS_AOF=%t", aofOn),
		"MINI_REDIS_AOF_PATH="+aofPath,
	)
	if token != "" {
		env = append(env, "MINI_REDIS_AUTH_TOKEN="+token)
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		_ = os.RemoveAll(aofDir)
		return nil, err
	}
	cmd := exec.Command(bin)
	cmd.Env = env
	cmd.Stdout = logFile
	stderr, err := cmd.StderrPipe()
	if err == nil {
		err = cmd.Start()
	}
	if err != nil {
		logFile.Close()
		_ = os.RemoveAll(aofDir)
		return nil, err
	}
	addrCh := make(chan string, 1)
	scanned := make(chan struct{})
	go func() {
		defer close(scanned)
		sc := bufio.NewScanner(stderr)
		sent := false
		for sc.Scan() {
			line := sc.Text()
			fmt.Fprintln(logFile, line)
			if !sent && strings.Contains(line, "msg=START") {
				for _, f := range strings.Fields(line) {
					if v, ok := strings.CutPrefix(f, "addr="); ok {
						addrCh <- v
						sent = true
					}
				}
			}
		}
	}()
	exited := make(chan error, 1)
	go func() {
		<-scanned // all pipe reads must finish before Wait closes the pipe
		err := cmd.Wait()
		logFile.Close()
		exited <- err
	}()
	// From here on every return path must go through stop, so the process and
	// its scratch directory are never left behind.
	stopped := false
	var result serverExit
	stop := func() serverExit {
		if stopped {
			return result
		}
		stopped = true
		result = serverExit{Status: "ok", Recorded: true}
		_ = cmd.Process.Signal(os.Interrupt) // graceful: flushes and closes the AOF
		var waitErr error
		select {
		case waitErr = <-exited:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			<-exited
			result.Status = "killed"
		}
		if result.Status == "ok" && waitErr != nil {
			result.Status = waitErr.Error()
		}
		if ps := cmd.ProcessState; ps != nil {
			result.CPU = ps.UserTime() + ps.SystemTime()
			result.MaxRSS = maxRSSBytes(ps)
		}
		_ = os.RemoveAll(aofDir)
		return result
	}
	select {
	case addr = <-addrCh:
	case err := <-exited:
		_ = os.RemoveAll(aofDir)
		return nil, fmt.Errorf("miniredis exited before start (%v); see %s", err, logPath)
	case <-time.After(15 * time.Second):
		stop()
		return nil, fmt.Errorf("miniredis did not report START; see %s", logPath)
	}
	t := &target{url: "http://" + addr, token: token, stop: stop}
	if aofOn {
		t.aofPath = aofPath
	}
	if _, err := waitHealthy(t.url, 10*time.Second); err != nil {
		stop()
		return nil, err
	}
	return t, nil
}

func freeLoopbackAddr() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer ln.Close()
	return ln.Addr().String(), nil
}

// waitHealthy polls /health and returns the reported persistence mode
// ("aof" or "disabled").
func waitHealthy(base string, limit time.Duration) (string, error) {
	deadline := time.Now().Add(limit)
	c := &http.Client{Timeout: time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/health", nil)
		res, err := c.Do(req)
		if err == nil {
			var body struct {
				Persistence string `json:"persistence"`
			}
			_ = json.NewDecoder(res.Body).Decode(&body)
			res.Body.Close()
			if res.StatusCode == http.StatusOK {
				cancel()
				return body.Persistence, nil
			}
			err = fmt.Errorf("HTTP %d", res.StatusCode)
		}
		cancel()
		if time.Now().After(deadline) {
			return "", fmt.Errorf("%s/health not ready: %v", base, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// aofWatch samples the AOF file from outside the server. mini-Redis exposes
// no sync/rewrite counters without debug logs, so a rewrite is inferred when
// the file is replaced (different inode) or shrinks between two samples.
type aofWatch struct {
	path     string
	stopCh   chan struct{}
	done     chan struct{}
	Start    int64
	End      int64
	Max      int64
	Rewrites int
}

func startAOFWatch(path string, begin time.Time) *aofWatch {
	w := &aofWatch{path: path, stopCh: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		select {
		case <-time.After(time.Until(begin)):
		case <-w.stopCh:
			return
		}
		prev, _ := os.Stat(path)
		if prev != nil {
			w.Start, w.Max, w.End = prev.Size(), prev.Size(), prev.Size()
		}
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		sample := func() {
			cur, err := os.Stat(path)
			if err != nil {
				return
			}
			if prev != nil && (!os.SameFile(prev, cur) || cur.Size() < prev.Size()) {
				w.Rewrites++
			}
			if cur.Size() > w.Max {
				w.Max = cur.Size()
			}
			w.End, prev = cur.Size(), cur
		}
		for {
			select {
			case <-tick.C:
				sample()
			case <-w.stopCh:
				sample()
				return
			}
		}
	}()
	return w
}

func (w *aofWatch) finish() {
	close(w.stopCh)
	<-w.done
}
