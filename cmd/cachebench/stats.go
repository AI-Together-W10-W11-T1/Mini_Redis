package main

import (
	"math"
	"sort"
	"strconv"
	"time"
)

func median(vals []float64) float64 {
	v := make([]float64, 0, len(vals))
	for _, x := range vals {
		if !math.IsNaN(x) && !math.IsInf(x, 0) {
			v = append(v, x)
		}
	}
	if len(v) == 0 {
		return math.NaN()
	}
	sort.Float64s(v)
	if len(v)%2 == 1 {
		return v[len(v)/2]
	}
	return (v[len(v)/2-1] + v[len(v)/2]) / 2
}

func minMax(vals []float64) (float64, float64) {
	lo, hi := math.NaN(), math.NaN()
	for _, x := range vals {
		if math.IsNaN(x) || math.IsInf(x, 0) {
			continue
		}
		if math.IsNaN(lo) || x < lo {
			lo = x
		}
		if math.IsNaN(hi) || x > hi {
			hi = x
		}
	}
	return lo, hi
}

// runRow is one line of runs.csv: one repeat of one condition.
type runRow struct {
	RunID         string
	Started       time.Time
	Scenario      string
	Cache         string
	AOF           string
	DelayMs       int
	Concurrency   int
	Repeat        int
	Order         int // position within its repeat (1 = ran first)
	Valid         bool
	Invalid       string
	Keys          int
	Requests      int64
	Successes     int64
	ElapsedS      float64
	SuccessRPS    float64
	MeanMs        float64
	P50, P95, P99 float64
	MaxMs         float64
	HitP50        float64
	MissP50       float64
	P99TailN      int64 // how many samples sit at or above p99
	Errors        int64
	Timeouts      int64
	Validation    int64
	SetFailures   int64
	Hits          int64
	Misses        int64
	HitRatio      float64
	OriginCalls   int64
	OriginPerReq  float64
	OriginPerKey  float64 // mean origin calls per key that was called at least once
	OriginKeyMax  int64
	KeysMultiCall int64 // keys fetched from origin more than once (stampede indicator)
	PrefillFails  int64
	CleanupFails  int
	ErrSamples    []string

	// Measuring tool, inside the measured interval.
	BenchCPUS     float64
	BenchCPUCores float64 // CPU seconds per wall second
	BenchGCCount  int64
	BenchGCPause  float64 // ms
	NewConns      int64   // DialContext calls inside the interval (should be 0)

	// Spawned server, whole process life (start-up, prefill, run, cleanup).
	ServerCPUS  float64
	ServerRSSMB float64
	ServerExit  string

	// AOF file observed from outside during the interval.
	SyncPeriods  int64 // whole seconds in the interval: an estimate, not a count of completed syncs
	AOFStart     int64
	AOFEnd       int64
	AOFMax       int64
	AOFRewrites  int64
	aofObserved  bool
	hasResources bool

	lat *hist // successful latencies, kept for pooled percentiles
}

func buildRow(spec runSpec, raw rawRun) runRow {
	rec := raw.rec
	if rec == nil {
		rec = &recorder{}
	}
	r := runRow{
		Scenario: spec.scenario, Concurrency: spec.concurrency, Keys: raw.keysTouched,
		Valid: raw.invalid == "", Invalid: raw.invalid,
		Requests: raw.requests, Successes: int64(rec.all.n),
		ElapsedS: raw.elapsed.Seconds(),
		MeanMs:   rec.all.meanMs(),
		P50:      rec.all.percentileMs(0.50), P95: rec.all.percentileMs(0.95), P99: rec.all.percentileMs(0.99),
		MaxMs:  rec.all.percentileMs(1.0),
		HitP50: rec.hit.percentileMs(0.50), MissP50: rec.miss.percentileMs(0.50),
		Errors: rec.errors, Timeouts: rec.timeouts, Validation: rec.validation, SetFailures: rec.setFailures,
		Hits: rec.hits, Misses: rec.misses, HitRatio: math.NaN(),
		OriginCalls: rec.originCalls, OriginPerKey: math.NaN(), OriginPerReq: math.NaN(),
		PrefillFails: raw.prefillFails, CleanupFails: raw.cleanupFails, ErrSamples: rec.errSamples,
		lat: &rec.all,
	}
	if n := rec.all.n; n > 0 {
		r.P99TailN = int64(n) - int64(math.Ceil(0.99*float64(n))) + 1
	}
	if raw.elapsed > 0 {
		r.SuccessRPS = float64(r.Successes) / raw.elapsed.Seconds()
	}
	if raw.requests > 0 && spec.scenario != "write" {
		r.OriginPerReq = float64(rec.originCalls) / float64(raw.requests)
	}
	if spec.cacheOn && spec.scenario != "write" && rec.hits+rec.misses > 0 {
		r.HitRatio = float64(rec.hits) / float64(rec.hits+rec.misses)
	}
	var called int64
	for _, n := range raw.perKey {
		if n > 0 {
			called++
		}
		if n > 1 {
			r.KeysMultiCall++
		}
		if n > r.OriginKeyMax {
			r.OriginKeyMax = n
		}
	}
	if called > 0 {
		r.OriginPerKey = float64(rec.originCalls) / float64(called)
	}
	if w := raw.win; w != nil {
		r.hasResources = true
		r.BenchCPUS = w.cpu.Seconds()
		if raw.elapsed > 0 {
			r.BenchCPUCores = w.cpu.Seconds() / raw.elapsed.Seconds()
		}
		r.BenchGCCount, r.BenchGCPause, r.NewConns = int64(w.gcCount), float64(w.gcPause)/1e6, w.dials
	}
	if a := raw.aof; a != nil {
		r.aofObserved = true
		r.AOFStart, r.AOFEnd, r.AOFMax, r.AOFRewrites = a.Start, a.End, a.Max, int64(a.Rewrites)
		r.SyncPeriods = int64(raw.elapsed / time.Second)
	}
	return r
}

type groupKey struct {
	Scenario    string
	Cache       string
	AOF         string
	DelayMs     int
	Concurrency int
}

func (r *runRow) key() groupKey {
	return groupKey{r.Scenario, r.Cache, r.AOF, r.DelayMs, r.Concurrency}
}

// summaryRow aggregates the valid repeats of one condition.
type summaryRow struct {
	groupKey
	Repeats, InvalidRuns      int
	RPSMedian, RPSMin, RPSMax float64
	RPSSpreadPct              float64 // (max-min)/median
	P50Med, P95Med, P99Med    float64 // median of per-repeat percentiles
	P50Pool, P95Pool, P99Pool float64 // percentile of all repeats' samples merged
	HitP50Med, MissP50Med     float64
	PooledSamples             int64
	Errors, Timeouts          int64
	Validation, SetFailures   int64
	CleanupFails              int64
	HitRatioMed               float64
	OriginCallsMed            float64
	OriginPerReqMed           float64
	OriginPerKeyMed           float64
	OriginKeyMax              int64
	KeysMultiCallMed          float64
	RequestsMed               float64
	ElapsedMed                float64
	BenchCPUCoresMed          float64
	BenchGCPauseMed           float64
	NewConns                  int64
	ServerCPUMed              float64
	ServerRSSMed              float64
	ServerExitBad             int
	SyncPeriodsMin            int64
	AOFRewrites               int64
	AOFEndMed                 float64

	// Paired comparison: each ON repeat against the cache-OFF run of the same
	// repeat (same scenario/delay/concurrency), then the median of the ratios.
	PairedOff                          int
	SpeedupP50, SpeedupP99             float64
	RPSRatio, RPSRatioMin, RPSRatioMax float64
	OriginReductionPct                 float64
	MissExtraMs                        float64 // miss p50 minus OFF p50
	// Paired AOF cost: each AOF-on repeat against the AOF-off run of the same repeat.
	PairedAOF                                   int
	AOFRPSRatio, AOFRPSRatioMin, AOFRPSRatioMax float64
	AOFP50DeltaMs, AOFP99DeltaMs                float64
	AOFMissDeltaMs                              float64 // miss p50, AOF on minus AOF off
}

func summarize(all []runRow) summaryRow {
	s := summaryRow{groupKey: all[0].key(), SyncPeriodsMin: -1}
	var rps, p50, p95, p99, hit, miss, hr, oc, opr, opk, kmc, req, el, bc, gp, sc, sr, ae []float64
	pooled := &hist{}
	for i := range all {
		r := &all[i]
		s.CleanupFails += int64(r.CleanupFails)
		if r.ServerExit != "" && r.ServerExit != "ok" {
			s.ServerExitBad++
		}
		if !r.Valid {
			s.InvalidRuns++
			continue
		}
		s.Repeats++
		rps = append(rps, r.SuccessRPS)
		p50 = append(p50, r.P50)
		p95 = append(p95, r.P95)
		p99 = append(p99, r.P99)
		hit = append(hit, r.HitP50)
		miss = append(miss, r.MissP50)
		hr = append(hr, r.HitRatio)
		oc = append(oc, float64(r.OriginCalls))
		opr = append(opr, r.OriginPerReq)
		opk = append(opk, r.OriginPerKey)
		kmc = append(kmc, float64(r.KeysMultiCall))
		req = append(req, float64(r.Requests))
		el = append(el, r.ElapsedS)
		if r.hasResources {
			bc = append(bc, r.BenchCPUCores)
			gp = append(gp, r.BenchGCPause)
			s.NewConns += r.NewConns
		}
		if r.ServerExit != "" {
			sc = append(sc, r.ServerCPUS)
			sr = append(sr, r.ServerRSSMB)
		}
		if r.aofObserved {
			ae = append(ae, float64(r.AOFEnd))
			s.AOFRewrites += r.AOFRewrites
			if s.SyncPeriodsMin < 0 || r.SyncPeriods < s.SyncPeriodsMin {
				s.SyncPeriodsMin = r.SyncPeriods
			}
		}
		pooled.merge(r.lat)
		s.Errors += r.Errors
		s.Timeouts += r.Timeouts
		s.Validation += r.Validation
		s.SetFailures += r.SetFailures
		if r.OriginKeyMax > s.OriginKeyMax {
			s.OriginKeyMax = r.OriginKeyMax
		}
	}
	s.RPSMedian = median(rps)
	s.RPSMin, s.RPSMax = minMax(rps)
	s.RPSSpreadPct = (s.RPSMax - s.RPSMin) / s.RPSMedian * 100
	s.P50Med, s.P95Med, s.P99Med = median(p50), median(p95), median(p99)
	s.P50Pool, s.P95Pool, s.P99Pool = pooled.percentileMs(0.5), pooled.percentileMs(0.95), pooled.percentileMs(0.99)
	s.PooledSamples = int64(pooled.n)
	s.HitP50Med, s.MissP50Med = median(hit), median(miss)
	s.HitRatioMed, s.OriginCallsMed, s.OriginPerKeyMed = median(hr), median(oc), median(opk)
	s.KeysMultiCallMed, s.RequestsMed, s.ElapsedMed = median(kmc), median(req), median(el)
	s.OriginPerReqMed = median(opr)
	s.BenchCPUCoresMed, s.BenchGCPauseMed = median(bc), median(gp)
	s.ServerCPUMed, s.ServerRSSMed, s.AOFEndMed = median(sc), median(sr), median(ae)
	nan := math.NaN()
	s.SpeedupP50, s.SpeedupP99, s.RPSRatio, s.RPSRatioMin, s.RPSRatioMax = nan, nan, nan, nan, nan
	s.OriginReductionPct, s.MissExtraMs = nan, nan
	s.AOFRPSRatio, s.AOFRPSRatioMin, s.AOFRPSRatioMax, s.AOFP50DeltaMs, s.AOFP99DeltaMs = nan, nan, nan, nan, nan
	s.AOFMissDeltaMs = nan
	return s
}

type pairKey struct {
	Scenario    string
	DelayMs     int
	Concurrency int
	Repeat      int
	Variant     string // "off", "on/off", "on/on"
}

func variantOf(r *runRow) string {
	if r.Cache == "off" {
		return "off"
	}
	return "on/" + r.AOF
}

// compare fills each summary with ratios computed repeat by repeat. A repeat
// runs all its variants back to back, so paired runs are at most a few
// minutes apart and drift between repeats cancels out.
func compare(rows []summaryRow, runs []runRow) {
	byPair := map[pairKey]*runRow{}
	for i := range runs {
		r := &runs[i]
		if r.Valid {
			byPair[pairKey{r.Scenario, r.DelayMs, r.Concurrency, r.Repeat, variantOf(r)}] = r
		}
	}
	for i := range rows {
		s := &rows[i]
		if s.Cache != "on" {
			continue
		}
		var sp50, sp99, rr, red, extra, arr, a50, a99, amiss []float64
		for j := range runs {
			r := &runs[j]
			if !r.Valid || r.key() != s.groupKey {
				continue
			}
			pk := pairKey{r.Scenario, r.DelayMs, r.Concurrency, r.Repeat, "off"}
			if off := byPair[pk]; off != nil {
				sp50 = append(sp50, off.P50/r.P50)
				sp99 = append(sp99, off.P99/r.P99)
				rr = append(rr, r.SuccessRPS/off.SuccessRPS)
				if off.OriginPerReq > 0 {
					red = append(red, (1-r.OriginPerReq/off.OriginPerReq)*100)
				}
				extra = append(extra, r.MissP50-off.P50)
			}
			if r.AOF == "on" {
				pk.Variant = "on/off"
				if base := byPair[pk]; base != nil {
					arr = append(arr, r.SuccessRPS/base.SuccessRPS)
					a50 = append(a50, r.P50-base.P50)
					a99 = append(a99, r.P99-base.P99)
					amiss = append(amiss, r.MissP50-base.MissP50)
				}
			}
		}
		s.PairedOff, s.PairedAOF = len(rr), len(arr)
		s.SpeedupP50, s.SpeedupP99, s.RPSRatio = median(sp50), median(sp99), median(rr)
		s.RPSRatioMin, s.RPSRatioMax = minMax(rr)
		s.OriginReductionPct, s.MissExtraMs = median(red), median(extra)
		s.AOFRPSRatio = median(arr)
		s.AOFRPSRatioMin, s.AOFRPSRatioMax = minMax(arr)
		s.AOFP50DeltaMs, s.AOFP99DeltaMs = median(a50), median(a99)
		s.AOFMissDeltaMs = median(amiss)
	}
}

func fmtF(v float64, prec int) string {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return ""
	}
	return strconv.FormatFloat(v, 'f', prec, 64)
}
