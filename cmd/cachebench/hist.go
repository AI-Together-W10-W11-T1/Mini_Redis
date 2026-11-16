package main

import (
	"math"
	"math/bits"
)

// hist is a fixed-size, mergeable latency histogram (log-linear buckets, like
// HdrHistogram). Recording a sample is one array increment and never
// allocates, so the measuring tool does not grow slices or feed the garbage
// collector during a run. Each power of two is split into 128 buckets, so a
// reported percentile is within 0.4% of the true sample value. Count, sum,
// min and max are exact.
type hist struct {
	counts [histBuckets]uint64
	n      uint64
	sum    float64
	min    int64
	max    int64
}

const (
	histSubBits = 7
	histMaxExp  = 40 // values above 2^41 ns (~36 min) are clamped
	histBuckets = (histMaxExp-histSubBits+1)<<histSubBits + 1<<histSubBits
)

func histIndex(v int64) int {
	if v < 0 {
		v = 0
	}
	u := uint64(v)
	if u < 1<<(histSubBits+1) {
		return int(u)
	}
	e := bits.Len64(u) - 1
	if e > histMaxExp {
		return histBuckets - 1
	}
	shift := uint(e - histSubBits)
	return int(shift)<<histSubBits + int(u>>shift)
}

// histValue returns the midpoint of a bucket.
func histValue(idx int) int64 {
	if idx < 1<<(histSubBits+1) {
		return int64(idx)
	}
	shift := uint(idx>>histSubBits) - 1
	m := int64(idx) - int64(shift)<<histSubBits
	return m<<shift + (int64(1)<<shift)/2
}

func (h *hist) add(ns int64) {
	h.counts[histIndex(ns)]++
	if h.n == 0 || ns < h.min {
		h.min = ns
	}
	if ns > h.max {
		h.max = ns
	}
	h.n++
	h.sum += float64(ns)
}

func (h *hist) merge(o *hist) {
	if o == nil || o.n == 0 {
		return
	}
	for i, c := range o.counts {
		h.counts[i] += c
	}
	if h.n == 0 || o.min < h.min {
		h.min = o.min
	}
	if o.max > h.max {
		h.max = o.max
	}
	h.n += o.n
	h.sum += o.sum
}

// percentileMs uses the nearest-rank method and returns milliseconds.
func (h *hist) percentileMs(p float64) float64 {
	if h == nil || h.n == 0 {
		return math.NaN()
	}
	rank := uint64(math.Ceil(p * float64(h.n)))
	if rank < 1 {
		rank = 1
	}
	if rank >= h.n {
		return float64(h.max) / 1e6
	}
	var seen uint64
	for i, c := range h.counts {
		seen += c
		if seen >= rank {
			v := histValue(i)
			if v < h.min {
				v = h.min
			}
			if v > h.max {
				v = h.max
			}
			return float64(v) / 1e6
		}
	}
	return float64(h.max) / 1e6
}

func (h *hist) meanMs() float64 {
	if h == nil || h.n == 0 {
		return math.NaN()
	}
	return h.sum / float64(h.n) / 1e6
}
