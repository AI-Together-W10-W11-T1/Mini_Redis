package store

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// The default configuration gives the AOF twice the data limit. With that
// headroom the data limit, not the file limit, must be what stops writes:
// clients then see CAPACITY_EXCEEDED rather than a persistence failure.
func TestAOFHeadroomLetsDataLimitBindFirst(t *testing.T) {
	const dataLimit = 1 << 20
	for _, valueBytes := range []int{0, 10, 1024, MaxValueBytes} {
		for _, withTTL := range []bool{false, true} {
			t.Run(fmt.Sprintf("value_%d_ttl_%v", valueBytes, withTTL), func(t *testing.T) {
				opts := DefaultAOFOptions(filepath.Join(t.TempDir(), "appendonly.aof"))
				opts.MaxBytes, opts.RewriteMinBytes = 2*dataLimit, dataLimit/4
				s, err := OpenPersistentWithLimit(nil, nil, opts, dataLimit)
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				value := strings.Repeat("x", valueBytes)
				set := func(i int) error {
					key := fmt.Sprintf("product:%d", i)
					if withTTL {
						ttl := int64(3_600_000)
						return s.Set(key, value, &ttl)
					}
					return s.Set(key, value, nil)
				}
				for i := 0; ; i++ {
					err := set(i)
					if err == nil {
						continue
					}
					if errors.Is(err, ErrPersistence) {
						// Overwrites and deletes are not involved, so only a
						// compaction of the append history may be needed.
						if rewriteErr := s.RewriteAOF(); rewriteErr != nil {
							t.Fatalf("rewrite after %d keys: %v", i, rewriteErr)
						}
						err = set(i)
					}
					if !errors.Is(err, ErrCapacityExceeded) {
						t.Fatalf("after %d keys the AOF limit bound before the data limit: %v", i, err)
					}
					return
				}
			})
		}
	}
}
