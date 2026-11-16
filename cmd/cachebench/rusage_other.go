//go:build !unix

package main

import (
	"os"
	"time"
)

func selfCPU() time.Duration { return 0 }

func maxRSSBytes(*os.ProcessState) int64 { return 0 }
