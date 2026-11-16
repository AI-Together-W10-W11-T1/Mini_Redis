package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

var envColumns = []string{"commit", "source_hash", "go_version", "os_arch", "cpu", "num_cpu", "server_mode",
	"server_bin_sha256", "server_go_version", "server_config"}

func envValues(e envInfo) []string {
	sha, gov := "", ""
	if e.ServerBinary != nil {
		sha, gov = e.ServerBinary.SHA256[:12], e.ServerBinary.GoVersion
	}
	return []string{e.Commit, e.SourceHash, e.GoVersion, e.OSArch, e.CPU, strconv.Itoa(e.NumCPU), e.ServerMode, sha, gov, e.ServerConfig}
}

var runColumns = []string{
	"run_id", "started_at", "scenario", "cache", "aof", "origin_delay_ms", "concurrency", "repeat", "order",
	"valid", "invalid_reason",
	"keys", "requests", "successes", "elapsed_s", "success_rps",
	"mean_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms", "hit_p50_ms", "miss_p50_ms", "p99_tail_samples",
	"errors", "timeouts", "validation_failures", "cache_set_failures",
	"hits", "misses", "hit_ratio", "origin_calls", "origin_calls_per_request", "origin_calls_per_key", "origin_calls_key_max", "keys_multi_origin",
	"prefill_failures", "cleanup_failures", "dial_attempts_in_window",
	"bench_cpu_s", "bench_cpu_cores", "bench_gc_count", "bench_gc_pause_ms",
	"server_cpu_s", "server_max_rss_mb", "server_exit",
	"aof_est_sync_periods", "aof_bytes_start", "aof_bytes_end", "aof_bytes_max", "aof_rewrites_seen",
	"error_samples",
}

type csvWriter struct {
	f *os.File
	w *csv.Writer
}

func newCSV(path string, header []string) (*csvWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := csv.NewWriter(f)
	if err := w.Write(header); err != nil {
		f.Close()
		return nil, err
	}
	w.Flush()
	return &csvWriter{f, w}, w.Error()
}

func (c *csvWriter) write(rec []string) error {
	if err := c.w.Write(rec); err != nil {
		return err
	}
	c.w.Flush() // flush per row: an interrupted session keeps every finished run
	return c.w.Error()
}

func (c *csvWriter) close() error { c.w.Flush(); return c.f.Close() }

func i64(v int64) string { return strconv.FormatInt(v, 10) }

func runRecord(r runRow, e envInfo) []string {
	res := []string{"", "", "", ""}
	if r.hasResources {
		res = []string{fmtF(r.BenchCPUS, 3), fmtF(r.BenchCPUCores, 2), i64(r.BenchGCCount), fmtF(r.BenchGCPause, 3)}
	}
	conns := ""
	if r.hasResources {
		conns = i64(r.NewConns)
	}
	srv := []string{"", "", ""}
	if r.ServerExit != "" {
		srv = []string{fmtF(r.ServerCPUS, 3), fmtF(r.ServerRSSMB, 1), r.ServerExit}
	}
	aof := []string{"", "", "", "", ""}
	if r.aofObserved {
		aof = []string{i64(r.SyncPeriods), i64(r.AOFStart), i64(r.AOFEnd), i64(r.AOFMax), i64(r.AOFRewrites)}
	}
	rec := []string{
		r.RunID, r.Started.UTC().Format(time.RFC3339), r.Scenario, r.Cache, r.AOF, strconv.Itoa(r.DelayMs),
		strconv.Itoa(r.Concurrency), strconv.Itoa(r.Repeat), strconv.Itoa(r.Order),
		strconv.FormatBool(r.Valid), r.Invalid,
		strconv.Itoa(r.Keys), i64(r.Requests), i64(r.Successes), fmtF(r.ElapsedS, 3), fmtF(r.SuccessRPS, 1),
		fmtF(r.MeanMs, 3), fmtF(r.P50, 3), fmtF(r.P95, 3), fmtF(r.P99, 3), fmtF(r.MaxMs, 3),
		fmtF(r.HitP50, 3), fmtF(r.MissP50, 3), i64(r.P99TailN),
		i64(r.Errors), i64(r.Timeouts), i64(r.Validation), i64(r.SetFailures),
		i64(r.Hits), i64(r.Misses), fmtF(r.HitRatio, 4), i64(r.OriginCalls), fmtF(r.OriginPerReq, 4), fmtF(r.OriginPerKey, 3),
		i64(r.OriginKeyMax), i64(r.KeysMultiCall), i64(r.PrefillFails), strconv.Itoa(r.CleanupFails), conns,
	}
	rec = append(rec, res...)
	rec = append(rec, srv...)
	rec = append(rec, aof...)
	rec = append(rec, strings.Join(r.ErrSamples, " | "))
	return append(rec, envValues(e)...)
}

var summaryColumns = []string{
	"scenario", "cache", "aof", "origin_delay_ms", "concurrency", "repeats", "invalid_runs", "requests_median", "elapsed_s_median",
	"success_rps_median", "success_rps_min", "success_rps_max", "rps_spread_pct",
	"p50_ms_median", "p95_ms_median", "p99_ms_median", "p50_ms_pooled", "p95_ms_pooled", "p99_ms_pooled", "pooled_samples",
	"hit_p50_ms_median", "miss_p50_ms_median",
	"errors_total", "timeouts_total", "validation_failures_total", "cache_set_failures_total", "cleanup_failures_total",
	"hit_ratio_median", "origin_calls_median", "origin_calls_per_request_median", "origin_calls_per_key_median",
	"origin_calls_key_max", "keys_multi_origin_median",
	"bench_cpu_cores_median", "bench_gc_pause_ms_median", "dial_attempts_in_window_total",
	"server_cpu_s_median", "server_max_rss_mb_median", "server_bad_exits",
	"aof_est_sync_periods_min", "aof_bytes_end_median", "aof_rewrites_seen_total",
	"paired_repeats_vs_off", "speedup_p50_vs_off", "speedup_p99_vs_off", "rps_ratio_vs_off", "rps_ratio_vs_off_min", "rps_ratio_vs_off_max",
	"origin_call_reduction_pct", "miss_extra_ms_vs_off",
	"paired_repeats_aof", "aof_rps_ratio_on_vs_off", "aof_rps_ratio_min", "aof_rps_ratio_max", "aof_p50_delta_ms", "aof_p99_delta_ms", "aof_miss_p50_delta_ms",
}

func summaryRecord(s summaryRow, e envInfo) []string {
	sync := ""
	if s.SyncPeriodsMin >= 0 {
		sync = i64(s.SyncPeriodsMin)
	}
	rec := []string{
		s.Scenario, s.Cache, s.AOF, strconv.Itoa(s.DelayMs), strconv.Itoa(s.Concurrency), strconv.Itoa(s.Repeats), strconv.Itoa(s.InvalidRuns),
		fmtF(s.RequestsMed, 0), fmtF(s.ElapsedMed, 3),
		fmtF(s.RPSMedian, 1), fmtF(s.RPSMin, 1), fmtF(s.RPSMax, 1), fmtF(s.RPSSpreadPct, 1),
		fmtF(s.P50Med, 3), fmtF(s.P95Med, 3), fmtF(s.P99Med, 3), fmtF(s.P50Pool, 3), fmtF(s.P95Pool, 3), fmtF(s.P99Pool, 3),
		i64(s.PooledSamples), fmtF(s.HitP50Med, 3), fmtF(s.MissP50Med, 3),
		i64(s.Errors), i64(s.Timeouts), i64(s.Validation), i64(s.SetFailures), i64(s.CleanupFails),
		fmtF(s.HitRatioMed, 4), fmtF(s.OriginCallsMed, 0), fmtF(s.OriginPerReqMed, 4), fmtF(s.OriginPerKeyMed, 3),
		i64(s.OriginKeyMax), fmtF(s.KeysMultiCallMed, 0),
		fmtF(s.BenchCPUCoresMed, 2), fmtF(s.BenchGCPauseMed, 3), i64(s.NewConns),
		fmtF(s.ServerCPUMed, 3), fmtF(s.ServerRSSMed, 1), strconv.Itoa(s.ServerExitBad),
		sync, fmtF(s.AOFEndMed, 0), i64(s.AOFRewrites),
		strconv.Itoa(s.PairedOff), fmtF(s.SpeedupP50, 2), fmtF(s.SpeedupP99, 2), fmtF(s.RPSRatio, 3), fmtF(s.RPSRatioMin, 3), fmtF(s.RPSRatioMax, 3),
		fmtF(s.OriginReductionPct, 1), fmtF(s.MissExtraMs, 3),
		strconv.Itoa(s.PairedAOF), fmtF(s.AOFRPSRatio, 3), fmtF(s.AOFRPSRatioMin, 3), fmtF(s.AOFRPSRatioMax, 3), fmtF(s.AOFP50DeltaMs, 3), fmtF(s.AOFP99DeltaMs, 3), fmtF(s.AOFMissDeltaMs, 3),
	}
	return append(rec, envValues(e)...)
}

var scenarioOrder = []string{"warm", "cold", "stampede", "write"}

func sortSummaries(rows []summaryRow) {
	scen := map[string]int{}
	for i, s := range scenarioOrder {
		scen[s] = i
	}
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a.Scenario != b.Scenario {
			return scen[a.Scenario] < scen[b.Scenario]
		}
		if a.DelayMs != b.DelayMs {
			return a.DelayMs < b.DelayMs
		}
		if a.Concurrency != b.Concurrency {
			return a.Concurrency < b.Concurrency
		}
		if a.Cache != b.Cache {
			return a.Cache == "off"
		}
		return a.AOF < b.AOF
	})
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func ms(v float64) string {
	if math.IsNaN(v) {
		return "-"
	}
	if math.Abs(v) < 1 {
		return fmtF(v, 3)
	}
	return fmtF(v, 2)
}

func signedMs(v float64) string {
	if math.IsNaN(v) {
		return "-"
	}
	if v >= 0 {
		return "+" + ms(v)
	}
	return ms(v)
}

func ratio(v float64) string {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return "-"
	}
	return fmtF(v, 2) + "×"
}

func ratioRange(med, lo, hi float64) string {
	if math.IsNaN(med) {
		return "-"
	}
	return fmt.Sprintf("%s (%s–%s)", ratio(med), fmtF(lo, 2), fmtF(hi, 2))
}

func pct(v float64) string {
	if math.IsNaN(v) {
		return "-"
	}
	return fmtF(v, 1) + "%"
}

func num(v float64, prec int) string {
	if math.IsNaN(v) {
		return "-"
	}
	return fmtF(v, prec)
}

func label(s summaryRow) string {
	if s.Cache == "off" {
		return "OFF"
	}
	return "ON (AOF " + s.AOF + ")"
}

var scenarioTitle = map[string]string{
	"warm":     "웜 캐시 (미리 채움, 시간 기준)",
	"cold":     "빈 캐시 (키당 첫 조회 1번, 요청 수 기준)",
	"stampede": "스탬피드 (같은 키 연속 요청, 요청 수 기준)",
	"write":    "지속 쓰기 (PUT만, 시간 기준, AOF 비용 비교용)",
}

// writeTable prints the condensed comparison tables (stdout and report.md).
func writeTable(w io.Writer, rows []summaryRow) {
	for _, scen := range scenarioOrder {
		var sel []summaryRow
		for _, r := range rows {
			if r.Scenario == scen {
				sel = append(sel, r)
			}
		}
		if len(sel) == 0 {
			continue
		}
		fmt.Fprintf(w, "\n### %s\n\n", scenarioTitle[scen])
		switch scen {
		case "warm":
			fmt.Fprintln(w, "| 원본 지연 | 동시 | 캐시 | 처리량 중앙값 (최소–최대) req/s | p50 | p95 | p99 | p99 합산 | 적중률 | 원본 호출/요청 | 실패 | p50 개선 | 처리량 배수 (최소–최대) | 원본 호출 감소 | 측정기 CPU 코어 | 서버 CPU s |")
			fmt.Fprintln(w, "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
		case "write":
			fmt.Fprintln(w, "| 동시 | AOF | 처리량 중앙값 (최소–최대) req/s | p50 | p95 | p99 | p99 합산 | 실패 | AOF on÷off 처리량 (최소–최대) | AOF p50 증가 | AOF p99 증가 | 추정 Sync 주기 (완료 횟수 아님) | 관측된 재작성 | AOF 끝 크기 MB | 측정기 CPU 코어 | 서버 CPU s |")
			fmt.Fprintln(w, "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
		default:
			fmt.Fprintln(w, "| 원본 지연 | 동시 | 캐시 | 측정 시간 s | 처리량 중앙값 (최소–최대) req/s | p50 | p95 | p99 합산 | 적중 p50 | 미스 p50 | 미스 추가 비용 | 적중률 | 키당 원본 호출 (평균/최대) | 실패 | 처리량 배수 | AOF 미스 p50 증가 |")
			fmt.Fprintln(w, "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
		}
		for _, r := range sel {
			hr := "-"
			if !math.IsNaN(r.HitRatioMed) {
				hr = pct(r.HitRatioMed * 100)
			}
			rps := fmt.Sprintf("%s (%s–%s)", num(r.RPSMedian, 0), num(r.RPSMin, 0), num(r.RPSMax, 0))
			fails := fmt.Sprintf("%d/%d/%d", r.Errors, r.Timeouts, r.Validation)
			if r.InvalidRuns > 0 {
				fails += fmt.Sprintf(" (무효 %d회)", r.InvalidRuns)
			}
			srvCPU := "-"
			if r.Cache == "on" {
				srvCPU = num(r.ServerCPUMed, 2)
			}
			switch scen {
			case "warm":
				fmt.Fprintf(w, "| %dms | %d | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
					r.DelayMs, r.Concurrency, label(r), rps, ms(r.P50Med), ms(r.P95Med), ms(r.P99Med), ms(r.P99Pool), hr,
					num(r.OriginPerReqMed, 3), fails, ratio(r.SpeedupP50), ratioRange(r.RPSRatio, r.RPSRatioMin, r.RPSRatioMax),
					pct(r.OriginReductionPct), num(r.BenchCPUCoresMed, 2), srvCPU)
			case "write":
				sync, rew, size := "-", "-", "-"
				if r.SyncPeriodsMin >= 0 {
					sync, rew, size = i64(r.SyncPeriodsMin), i64(r.AOFRewrites), num(r.AOFEndMed/(1<<20), 1)
				}
				fmt.Fprintf(w, "| %d | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
					r.Concurrency, r.AOF, rps, ms(r.P50Med), ms(r.P95Med), ms(r.P99Med), ms(r.P99Pool), fails,
					ratioRange(r.AOFRPSRatio, r.AOFRPSRatioMin, r.AOFRPSRatioMax), signedMs(r.AOFP50DeltaMs), signedMs(r.AOFP99DeltaMs),
					sync, rew, size, num(r.BenchCPUCoresMed, 2), srvCPU)
			default:
				fmt.Fprintf(w, "| %dms | %d | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s/%d | %s | %s | %s |\n",
					r.DelayMs, r.Concurrency, label(r), num(r.ElapsedMed, 2), rps, ms(r.P50Med), ms(r.P95Med), ms(r.P99Pool),
					ms(r.HitP50Med), ms(r.MissP50Med), signedMs(r.MissExtraMs), hr, num(r.OriginPerKeyMed, 2), r.OriginKeyMax, fails,
					ratio(r.RPSRatio), signedMs(r.AOFMissDeltaMs))
			}
		}
	}
	fmt.Fprintln(w, "\n지연 단위는 ms입니다. p50·p95·p99는 반복마다 그 반복의 지연 분포에서 구한 백분위수의 중앙값이고(백분위수끼리 평균 내지 않음), \"p99 합산\"은 모든 반복의 분포를 합쳐 다시 구한 p99입니다. 실패는 오류/타임아웃/검증 실패 순서입니다. 개선·배수는 **같은 반복 안에서 바로 이어 측정한** 캐시 OFF 실행과 짝지어 구한 비율의 중앙값이며 1보다 크면 ON이 유리합니다. \"AOF …\" 열은 같은 반복의 AOF off 실행과 짝지은 값입니다. \"미스 추가 비용\"은 ON의 미스 p50에서 OFF의 p50을 뺀 값입니다.")
}

// warnings flags results that should not be read at face value. Problems
// that invalidate a number are listed one by one; conditions that only need
// care are grouped into one line each.
func warnings(rows []summaryRow, runs []runRow, env envInfo) []string {
	var out []string
	for _, r := range runs {
		if !r.Valid {
			out = append(out, fmt.Sprintf("%s (%s / %dms / 동시 %d / cache %s / aof %s / 반복 %d): 무효 처리되어 요약에서 뺐습니다. 사유: %s", r.RunID, r.Scenario, r.DelayMs, r.Concurrency, r.Cache, r.AOF, r.Repeat, r.Invalid))
		}
		if r.ServerExit != "" && r.ServerExit != "ok" {
			out = append(out, fmt.Sprintf("%s: mini-Redis가 정상 종료하지 않았습니다(%s). server-aof-%s.log를 확인하세요.", r.RunID, r.ServerExit, r.AOF))
		}
	}
	var spread, benchCPU, shortAOF []string
	for _, r := range rows {
		name := fmt.Sprintf("%s/%dms/동시 %d/%s", r.Scenario, r.DelayMs, r.Concurrency, label(r))
		if r.Repeats == 0 {
			continue
		}
		if r.Errors+r.Timeouts+r.Validation > 0 {
			out = append(out, fmt.Sprintf("%s: 실패 %d건(오류 %d, 타임아웃 %d, 검증 %d). 실패는 지연·처리량 집계에서 빠져 있으니 runs.csv의 error_samples를 먼저 확인하세요.", name, r.Errors+r.Timeouts+r.Validation, r.Errors, r.Timeouts, r.Validation))
		}
		if r.SetFailures > 0 {
			out = append(out, fmt.Sprintf("%s: 캐시 저장(PUT) 실패 %d건. 응답은 원본 값으로 성공 처리했지만 캐시가 채워지지 않았습니다.", name, r.SetFailures))
		}
		if r.CleanupFails > 0 {
			out = append(out, fmt.Sprintf("%s: 측정 후 키 정리(DELETE) 실패 %d건. 외부 서버라면 `bench:` 접두사 키가 TTL까지 남습니다.", name, r.CleanupFails))
		}
		if r.NewConns > 0 {
			out = append(out, fmt.Sprintf("%s: 측정 구간 안에서 새 TCP 연결 시도가 %d번 있었습니다. 연결 비용이 지연에 섞였을 수 있습니다.", name, r.NewConns))
		}
		if r.Scenario == "warm" && r.Cache == "on" && !math.IsNaN(r.HitRatioMed) && r.HitRatioMed < 0.999 {
			out = append(out, fmt.Sprintf("%s: 웜 캐시인데 적중률이 %s입니다. 측정 중 키가 사라졌는지(TTL·용량) 확인하세요.", name, pct(r.HitRatioMed*100)))
		}
		if r.Cache == "off" && r.DelayMs > 0 {
			bound := float64(r.Concurrency) * 1000 / float64(r.DelayMs)
			if r.RPSMedian < bound*0.8 {
				out = append(out, fmt.Sprintf("%s: 캐시 OFF 처리량 %s req/s가 이론 상한(동시 %d × 1000/%dms = %s req/s)의 80%% 미만입니다. 원본 모형이나 클라이언트가 병목인지 확인하세요.", name, fmtF(r.RPSMedian, 0), r.Concurrency, r.DelayMs, fmtF(bound, 0)))
			}
		}
		if r.Cache == "on" && r.PairedOff > 0 && r.PairedOff < r.Repeats {
			out = append(out, fmt.Sprintf("%s: OFF와 짝지어진 반복이 %d/%d회뿐입니다.", name, r.PairedOff, r.Repeats))
		}
		// Throughput of a run shorter than a second is not a meaningful figure,
		// so its spread is not reported.
		if r.RPSSpreadPct > 10 && r.ElapsedMed >= 1 {
			spread = append(spread, fmt.Sprintf("%s(%s)", name, pct(r.RPSSpreadPct)))
		}
		if env.NumCPU > 0 && r.BenchCPUCoresMed > 0.5*float64(env.NumCPU) {
			benchCPU = append(benchCPU, fmt.Sprintf("%s(%s코어)", name, num(r.BenchCPUCoresMed, 2)))
		}
		if r.Cache == "on" && r.AOF == "on" && r.SyncPeriodsMin == 0 {
			shortAOF = append(shortAOF, name)
		}
	}
	if len(spread) > 0 {
		out = append(out, "반복 간 처리량 편차가 10%를 넘는 조건: "+strings.Join(spread, ", ")+". 원인은 이 수치만으로 알 수 없습니다. 측정기 CPU와 서버 CPU, 다른 프로그램 부하, 발열을 함께 확인하고 처리량은 최소–최대 범위로 읽으세요.")
	}
	if len(benchCPU) > 0 {
		out = append(out, fmt.Sprintf("측정기(원본 모형 포함)가 전체 %d코어의 절반 넘게 쓴 조건: %s. 이 조건의 처리량은 mini-Redis가 아니라 측정기와 CPU 경합이 한계일 수 있으니, 서버의 최대 처리량으로 인용하지 마세요.", env.NumCPU, strings.Join(benchCPU, ", ")))
	}
	if len(shortAOF) > 0 {
		out = append(out, "측정 구간이 1초보다 짧아 AOF의 1초 주기 Sync를 온전히 포함하지 못한 조건: "+strings.Join(shortAOF, ", ")+". 이 행들로 AOF 비용을 판단하지 말고 지속 쓰기(write) 시나리오를 보세요.")
	}
	var lowTail []string
	seen := map[groupKey]bool{}
	for _, r := range runs {
		if k := r.key(); r.Valid && r.P99TailN < 10 && !seen[k] {
			seen[k] = true
			name := "OFF"
			if k.Cache == "on" {
				name = "ON (AOF " + k.AOF + ")"
			}
			lowTail = append(lowTail, fmt.Sprintf("%s/%dms/동시 %d/%s", k.Scenario, k.DelayMs, k.Concurrency, name))
		}
	}
	if len(lowTail) > 0 {
		out = append(out, "반복별 p99가 표본 10개 미만에 좌우되는 조건: "+strings.Join(lowTail, ", ")+". p99는 \"p99 합산\" 값을 우선 보고 꼬리 지연 결론은 조심하세요.")
	}
	return out
}

type planInfo struct {
	Scenarios     []string `json:"scenarios"`
	Caches        []string `json:"cache"`
	AOFs          []string `json:"aof"`
	DelaysMs      []int    `json:"origin_delays_ms"`
	Concurrency   []int    `json:"concurrency"`
	Repeats       int      `json:"repeats"`
	Products      int      `json:"products"`
	ValueBytes    int      `json:"value_bytes"`
	Warmup        string   `json:"warmup"`
	Duration      string   `json:"duration"`
	WriteDuration string   `json:"write_duration"`
	ColdKeys      int      `json:"cold_keys"`
	StampedeKeys  int      `json:"stampede_keys"`
	StampedePer   int      `json:"stampede_requests_per_key"`
	Timeout       string   `json:"request_timeout"`
	Seed          int64    `json:"seed"`
}

func writeReport(path string, env envInfo, plan planInfo, rows []summaryRow, runs []runRow, interrupted bool) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := f
	has := func(s string) bool { return contains(plan.Scenarios, s) }
	fmt.Fprintf(w, "# 캐시 사용·미사용 성능 비교 결과\n\n")
	if interrupted {
		fmt.Fprintf(w, "> **중단된 측정입니다.** 끝난 반복만 집계했습니다.\n\n")
	}
	fmt.Fprintf(w, "## 측정 환경\n\n")
	fmt.Fprintf(w, "| 항목 | 값 |\n|---|---|\n")
	fmt.Fprintf(w, "| 커밋 | `%s` |\n| 소스 해시 (.go·go.mod) | `%s` |\n| 측정기 Go | %s |\n| OS/아키텍처 | %s |\n| CPU | %s (논리 코어 %d, GOMAXPROCS %d) |\n",
		env.Commit, env.SourceHash, env.GoVersion, env.OSArch, env.CPU, env.NumCPU, env.GOMAXPROCS)
	fmt.Fprintf(w, "| mini-Redis 실행 | %s, 인증 %t |\n| 서버 빌드 | %s |\n| 서버 설정 | %s |\n", env.ServerMode, env.Auth, env.ServerBuild, env.ServerConfig)
	if b := env.ServerBinary; b != nil {
		var keys []string
		for k := range b.Settings {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var kv []string
		for _, k := range keys {
			kv = append(kv, k+"="+b.Settings[k])
		}
		fmt.Fprintf(w, "| 서버 바이너리 | sha256 `%s`, %s, %s |\n", b.SHA256[:12], b.GoVersion, strings.Join(kv, " "))
	}
	if env.Label != "" {
		fmt.Fprintf(w, "| 메모 | %s |\n", env.Label)
	}
	fmt.Fprintf(w, "| 배치 | %s |\n", env.Topology)
	fmt.Fprintf(w, "\n## 조건\n\n상품 응답 약 %d바이트, 원본 지연 %v ms, 동시 요청 %v, 반복 %d회, 요청 타임아웃 %s. 한 반복 안에서 캐시 OFF·ON(AOF별)을 연달아 측정하고 반복마다 그 순서를 한 칸씩 돌립니다. 캐시 ON 실행은 매번 새로 띄운 mini-Redis(빈 메모리, 빈 AOF)에서 합니다.\n\n",
		plan.ValueBytes, plan.DelaysMs, plan.Concurrency, plan.Repeats, plan.Timeout)
	if has("warm") {
		fmt.Fprintf(w, "- **웜 캐시(warm)**: 캐시 ON은 상품 %d개를 한 번씩 조회해 채우고 전부 다시 읽어 저장을 확인한 뒤(하나라도 실패하면 그 실행은 무효) 준비 %s, 본 측정 %s. 키는 균등 분포로 고릅니다.\n", plan.Products, plan.Warmup, plan.Duration)
	}
	if has("cold") {
		fmt.Fprintf(w, "- **빈 캐시(cold)**: 서로 다른 키 %d개를 한 번씩 조회합니다. ON에서는 모든 요청이 첫 조회(미스)입니다. 요청을 다 처리하면 끝납니다.\n", plan.ColdKeys)
	}
	if has("stampede") {
		fmt.Fprintf(w, "- **스탬피드(stampede)**: 키 %d개를 키당 %d번씩 연속 배치한 요청 %d건. 동시 요청이 겹치면 같은 키에 미스가 몰립니다.\n", plan.StampedeKeys, plan.StampedePer, plan.StampedeKeys*plan.StampedePer)
	}
	if has("write") {
		fmt.Fprintf(w, "- **지속 쓰기(write)**: 키 %d개에 PUT만 준비 %s, 본 측정 %s 동안 계속 보냅니다. 원본과 캐시 OFF는 관여하지 않으며 AOF off와 on을 비교합니다. 끝나면 PUT이 성공한 모든 키를 원본 우회 없이 다시 읽어 검증하고, 값이 없거나 다르면 그 실행을 무효로 합니다.\n", plan.Products, plan.Warmup, plan.WriteDuration)
	}

	fmt.Fprintf(w, "\n## 결과 요약\n")
	writeTable(w, rows)

	fmt.Fprintf(w, "\n## 자동 점검\n\n")
	if ws := warnings(rows, runs, env); len(ws) == 0 {
		fmt.Fprintln(w, "특이 사항이 없습니다.")
	} else {
		for _, s := range ws {
			fmt.Fprintf(w, "- %s\n", s)
		}
	}

	fmt.Fprintf(w, "\n## 미리 적어 둔 예상치\n\n")
	fmt.Fprintln(w, "- **원본 0ms, 웜 캐시**: ON과 OFF 모두 HTTP 왕복 한 번입니다. ON은 인증 검사와 저장값 JSON 한 겹을 더 풀어야 하므로 **비슷하거나 ON이 약간 느린 것이 정상**입니다. 이 조건에서 ON이 크게 빠르면 측정을 의심하세요.")
	fmt.Fprintln(w, "- **원본 0ms, 빈 캐시**: ON의 미스는 캐시 조회 → 원본 → 캐시 저장으로 왕복이 세 번이라 **ON이 OFF보다 느립니다.**")
	for _, d := range plan.DelaysMs {
		if d <= 0 {
			continue
		}
		fmt.Fprintf(w, "- **원본 %dms, 웜 캐시**: OFF의 p50은 %dms보다 조금 크고, 처리량 상한은 동시 수 × 1000/%d req/s입니다(동시 1이면 약 %.0f, 동시 20이면 약 %.0f). ON은 원본을 부르지 않으므로 p50이 1ms 안팎이고 처리량은 mini-Redis와 이 기기의 CPU가 결정합니다. 이 배수는 **인위적인 %dms 대기를 건너뛴 효과**이지 mini-Redis가 일반적으로 그만큼 빠르다는 뜻이 아닙니다.\n",
			d, d, d, 1000/float64(d), 20*1000/float64(d), d)
		fmt.Fprintf(w, "- **원본 %dms, 빈 캐시**: ON의 미스는 OFF보다 캐시 왕복 두 번만큼 느립니다. 스탬피드에서는 동시 1이면 키당 원본 호출이 1이지만, 동시 요청이 겹치면 1보다 커집니다(동시 미스를 합치는 기능이 없기 때문).\n", d)
	}
	if has("write") {
		fmt.Fprintln(w, "- **지속 쓰기**: 표의 배수는 기록 덧붙이기·동기화·재작성이 합쳐진 \"AOF가 켜진 상태에서 관측한 비용\"이며 항목별로 나눌 수 없습니다. AOF on은 PUT마다 기록을 파일에 덧붙이므로 AOF off보다 처리량이 낮고 지연이 큰 것이 정상입니다. 디스크 동기화(fsync)는 1초 주기로 따로 일어나므로 개별 PUT의 지연에는 직접 포함되지 않고, 동기화나 재작성이 도는 순간의 꼬리 지연(p99)으로 나타납니다.")
	}

	fmt.Fprintf(w, "\n## 해석할 때의 한계\n\n")
	fmt.Fprintln(w, "- **같은 기기·루프백 측정입니다.** 측정 CLI, 원본 모형, mini-Redis가 한 기기의 CPU를 나눠 쓰고 네트워크는 루프백입니다. 실제 배포에서는 캐시 왕복에 네트워크 지연이 더해지므로 ON의 이득은 이 결과보다 작아지고, 0ms 조건에서 ON의 손해는 커집니다.")
	fmt.Fprintln(w, "- **측정기도 부하입니다.** 원본 모형이 측정기 프로세스 안에 있어 측정기의 CPU 사용과 GC가 OFF·ON 양쪽에 영향을 줍니다. 표의 \"측정기 CPU 코어\"가 전체 코어의 절반을 넘는 조건의 처리량은 서버가 아니라 측정기의 한계일 수 있습니다. 지연 표본은 고정 크기 히스토그램에 기록하므로 표본 기록 때문에 메모리가 늘지는 않습니다(HTTP 요청과 JSON 처리의 할당은 그대로 있습니다). 백분위수의 상대 오차는 0.4% 이하입니다.")
	fmt.Fprintln(w, "- **원본 지연은 인위적입니다.** 원본은 `time.Sleep`으로 DB·외부 API 비용을 흉내 낼 뿐, 실제 DB처럼 부하에 따라 느려지지 않습니다.")
	fmt.Fprintf(w, "- **균등 분포·상품 %d개의 웜 캐시는 적중률 100%%인 최선의 경우입니다.** 키 수가 캐시보다 많거나 인기 편중·TTL 만료·무효화가 있으면 적중률이 내려갑니다.\n", plan.Products)
	fmt.Fprintln(w, "- **AOF 수치는 \"관측한 비용\"입니다.** 웜 캐시는 거의 GET만 하므로 AOF 차이가 나와도 AOF 비용이 아닙니다. 빈 캐시·스탬피드는 측정 구간이 짧아 1초 주기 Sync를 포함하지 못할 수 있습니다(표의 측정 시간 참고). AOF 비용은 지속 쓰기 시나리오에서 보되, 이 값도 \"쓰기 응답이 돌아올 때까지의 지연\"이지 \"디스크 동기화가 끝날 때까지의 지연\"이 아닙니다. \"추정 Sync 주기\"는 측정 구간의 초 수일 뿐 Sync가 그만큼 실행·완료됐다는 증거가 아니며, 재작성은 AOF 파일이 교체되거나 줄어든 것을 50ms 간격으로 관찰해 셉니다.")
	fmt.Fprintln(w, "- **동시 요청 수를 고정한 닫힌 루프 측정입니다.** 처리량은 \"이 동시성에서 얻은 처리량\"이며, 특정 요청률을 견디는지 보는 부하 시험과 다릅니다. 서버가 느려지면 요청도 덜 보내므로 꼬리 지연이 실제보다 작게 보일 수 있습니다.")
	fmt.Fprintln(w, "- **적중·미스는 클라이언트가 센 값입니다.** mini-Redis는 통계 API가 없으므로 GET 응답 200을 적중, 404를 미스로 셉니다.")
	fmt.Fprintln(w, "- **서버 CPU·메모리는 프로세스 전체 수명 기준입니다.** 실행마다 서버를 새로 띄우므로 시작, 미리 채우기, 측정, 정리가 모두 포함됩니다. 측정기 CPU는 측정 구간만 잽니다.")
	fmt.Fprintln(w, "- 실패한 요청(오류·타임아웃·검증 실패)은 지연·처리량에서 빼고 따로 셉니다. 실패가 0이 아니면 그 조건의 속도 수치는 비교에 쓰지 마세요.")

	fmt.Fprintf(w, "\n## 파일\n\n- `runs.csv`: 실행 하나가 한 줄입니다. 무효 실행도 `valid=false`로 남습니다.\n- `summary.csv`: 조건별 중앙값·최소·최대·합산 백분위수, 같은 반복끼리 짝지은 OFF 대비·AOF off 대비 비교입니다.\n- `meta.json`: 환경, 서버 바이너리 해시와 빌드 정보, 조건 전체입니다.\n- `server-aof-*.log`: 측정용 mini-Redis 로그입니다.\n")
	return nil
}

func ensureDir(p string) error { return os.MkdirAll(filepath.Clean(p), 0o755) }
