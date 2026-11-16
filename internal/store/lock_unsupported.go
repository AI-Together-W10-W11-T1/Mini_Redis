//go:build !darwin && !linux

package store

import (
	"errors"
	"os"
)

func lockFile(*os.File) error {
	return errors.New("AOF persistence is supported only on Linux and macOS; set MINI_REDIS_AOF=false")
}
