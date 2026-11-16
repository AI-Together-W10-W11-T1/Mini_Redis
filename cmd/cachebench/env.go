package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

// envInfo identifies exactly what was measured and where.
type envInfo struct {
	Commit      string `json:"commit"`       // short HEAD hash, "-dirty" if the tree has changes
	SourceHash  string `json:"source_hash"`  // sha256 of all .go files + go.mod (identifies uncommitted code)
	GoVersion   string `json:"go_version"`   // toolchain of this CLI (and of the spawned server build)
	OSArch      string `json:"os_arch"`      //
	CPU         string `json:"cpu"`          //
	NumCPU      int    `json:"num_cpu"`      //
	GOMAXPROCS  int    `json:"gomaxprocs"`   //
	ServerMode  string `json:"server_mode"`  // spawn (CLI started mini-Redis) or external
	Auth        bool   `json:"auth"`         // Bearer auth enabled on mini-Redis
	Label       string `json:"label"`        // free-form note from -label
	Topology    string `json:"topology"`     // where CLI, origin and mini-Redis ran
	OriginModel string `json:"origin_model"` //

	// What is known about the measured server itself.
	ServerBuild  string      `json:"server_build"`            // who built the binary, or that it is external
	ServerConfig string      `json:"server_config"`           // tool-set vs user-declared
	ServerBinary *binaryInfo `json:"server_binary,omitempty"` // hash and build info of the spawned executable
}

func findModuleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("go.mod not found; run from inside the AI-Redis module")
		}
		dir = parent
	}
}

func gitCommit(root string) string {
	out, err := exec.Command("git", "-C", root, "rev-parse", "--short=12", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	commit := strings.TrimSpace(string(out))
	st, err := exec.Command("git", "-C", root, "status", "--porcelain", "--untracked-files=normal").Output()
	if err == nil && len(strings.TrimSpace(string(st))) > 0 {
		commit += "-dirty"
	}
	return commit
}

func sourceHash(root string) string {
	var files []string
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		name := d.Name()
		if d.IsDir() && p != root && (strings.HasPrefix(name, ".") || name == "data" || name == "bench-results") {
			return filepath.SkipDir
		}
		if !d.IsDir() && (strings.HasSuffix(name, ".go") || name == "go.mod" || name == "go.sum") {
			files = append(files, p)
		}
		return nil
	})
	sort.Strings(files)
	h := sha256.New()
	for _, f := range files {
		rel, _ := filepath.Rel(root, f)
		io.WriteString(h, filepath.ToSlash(rel)+"\x00")
		if b, err := os.ReadFile(f); err == nil {
			h.Write(b)
		}
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:12]
}

func cpuModel() string {
	switch runtime.GOOS {
	case "darwin":
		if out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output(); err == nil {
			return strings.TrimSpace(string(out))
		}
	case "linux":
		if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
			for _, line := range strings.Split(string(b), "\n") {
				if k, v, ok := strings.Cut(line, ":"); ok && strings.TrimSpace(k) == "model name" {
					return strings.TrimSpace(v)
				}
			}
		}
	}
	return "unknown"
}

func collectEnv(root, mode string, auth bool, label string) envInfo {
	return envInfo{
		Commit: gitCommit(root), SourceHash: sourceHash(root),
		GoVersion: runtime.Version(), OSArch: runtime.GOOS + "/" + runtime.GOARCH,
		CPU: cpuModel(), NumCPU: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0),
		ServerMode: mode, Auth: auth, Label: label,
		Topology:    "same-host loopback: CLI + origin (in CLI process) + mini-Redis share one machine's CPU",
		OriginModel: "in-process HTTP origin, immutable catalog, lock-free, artificial time.Sleep delay",
	}
}
