//go:build unix

package main

import (
	"os"
	"runtime"
	"syscall"
	"time"
)

// selfCPU returns the user+system CPU time this process has consumed.
func selfCPU() time.Duration {
	var ru syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// maxRSSBytes reads the peak resident set size of an exited child.
func maxRSSBytes(ps *os.ProcessState) int64 {
	ru, ok := ps.SysUsage().(*syscall.Rusage)
	if !ok || ru == nil {
		return 0
	}
	if runtime.GOOS == "darwin" || runtime.GOOS == "ios" {
		return int64(ru.Maxrss) // bytes on macOS
	}
	return int64(ru.Maxrss) * 1024 // kilobytes elsewhere
}
