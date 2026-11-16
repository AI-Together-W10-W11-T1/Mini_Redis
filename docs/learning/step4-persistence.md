# 4단계: AOF + 1초 주기 Sync

## 1. 결정과 보장 범위

2026-11-16 사용자 요청에 따라 영속성 없음에서 AOF + 1초 주기 Sync로 변경했다. 정상 종료 시 최종 Sync를 수행한다. 강제 종료 후에는 읽을 수 있는 완전한 기록을 순서대로 복구한다.

**PUT·DELETE의 성공은 파일 추가 기록과 메모리 반영 성공을 뜻하며, 해당 요청의 디스크 Sync 완료를 뜻하지 않는다.** 보통 마지막 Sync 이후 변경 일부가 유실될 수 있다. 1초는 목표 주기이며 손실 상한 보장은 아니다. 전원 차단·디스크 고장·파일 삭제·벽시계 오류에 대한 무손실 보장은 없다.

파일 쓰기/Sync 오류는 기록 상태를 불확실하게 만들므로 이후 변경을 거부한다. 조회는 유지하고 운영자가 원인을 해결한 뒤 재시작한다. 오류 응답 또는 응답 도중 단절된 요청은 재시작 후 반영돼 있을 수도 있다. 자동 재시도나 exactly-once 보장은 제공하지 않는다.

### 검수 반영: 변경 요청 대기와 취소

영속성 변경 요청은 저장 규칙 진입 후 **기본 3초까지 락 대기와 실행 결과를 기다린다.** HTTP 요청의 더 짧은 deadline·취소도 전달한다. 서버의 WriteTimeout을 핸들러 취소 기능으로 간주하지 않는다.

- 변경 순서 락을 아직 얻지 못했다면 기한 후 503 `PERSISTENCE_UNAVAILABLE`로 종료하고, 해당 요청을 나중에 실행하지 않는다.
- 이미 실행에 들어간 파일 쓰기는 OS에서 취소할 수 없다. 호출자는 기한 후 503 `PERSISTENCE_OUTCOME_UNKNOWN` 또는 연결 오류를 볼 수 있지만 **변경이 나중에 완료될 수 있다.** 결과 불명 상태이며 자동 재시도하지 않는다.
- 작업은 완료할 때까지 변경 순서 락을 유지한다. 느린 요청마다 새 파일 작업을 무제한 생성하지 않으며 Store당 변경 작업자는 최대 하나다. 후속 변경은 기한 후 거부하고 조회는 유지한다.
- SET·DELETE의 기존 메서드는 유지하고 SetContext·DeleteContext를 추가했다. 메모리 전용 모드도 락 대기 취소를 지원한다.

## 2. 비용·한계 대응

| 문제 | 구현한 대응 | 남는 비용·한계 |
|---|---|---|
| 매 요청 Sync 지연 | 백그라운드 1초 주기 Sync | 최근 미동기화 변경 손실 가능 |
| 디스크 대기로 조회 중단 | 메모리 락과 변경 순서 락 분리, Sync·파일 I/O는 메모리 락 밖 | 새 쓰기는 파일 I/O·교체 완료를 기다릴 수 있음. 운영체제 자원 경합은 남음 |
| Sync 장기 지연 | 미동기화 기록이 3초 넘으면 변경에 503. 동기화 회복 시 재개 | 앞서 성공 응답한 변경의 위험을 소급 제거하지 못함 |
| 변경 순서 락·파일 쓰기 장기 대기 | 기본 3초의 호출자 대기 제한, 요청 context 전달 | 시작한 쓰기는 늦게 완료될 수 있음. 파일 I/O 자체를 취소하는 보장은 아님 |
| 파일 계속 증가 | 유효 데이터만 새 파일에 기록하고 동시 변경분을 합치는 자동 재작성 | 메모리 락은 최대 128개의 버킷 탐색·노드 방문 단위로 나누고 목록 확장은 락 밖에서 처리. 전체 복사 비용·GC 지연·쓰기 대기는 남음 |
| 재작성으로 자원 폭증 | 변경분 버퍼 8 MiB 상한, 초과하면 기존 파일 유지. 실패 재시도 1~60초 지수 지연 | 높은 쓰기 부하에서는 재작성 실패가 반복될 수 있음 |
| 파일 한도에서 삭제도 막힘 | SET을 수락할 때 현재 키의 DEL 기록 공간까지 예약. 실제 파일+예약량을 256 MiB 안에 유지 | 저장 가능한 데이터량은 줄어듦. 예약은 정책상 여유이며 물리 디스크 공간을 미리 할당한 것은 아님 |
| 중간 종료·손상 | 프레임 길이·본문 각각 CRC32 검사. 마지막 불완전 기록만 절단. 완전한 기록의 손상은 시작 거부 | 원본 파일을 임의로 고치거나 손상 레코드를 건너뛰지 않음 |
| 두 서버가 같은 파일 기록 | 별도 잠금 파일에 OS 잠금 | 현재 로컬 Mac/Linux 파일 시스템 대상. 네트워크 파일 시스템·Windows는 보장 대상 아님 |
| 종료 중 남은 임시 파일 | 시작 시 같은 AOF의 잠금을 얻고 그 파일의 재작성 잔여물만 정리 | 원본 AOF 검증 실패 시 임시 파일도 보존 |
| 재시작 시 TTL 연장 | 최초 TTL 대신 절대 만료 시각 저장 | 실행 중 단조 시계는 유지하지만 재시작 경계에서는 정확한 시스템 시각에 의존 |

로그 재작성은 Redis 파일 포맷·fork를 복제한 구현이 아니다. 내부 포맷은 `MINIAOF1` + 길이·길이 체크섬·JSON 본문·본문 체크섬의 반복이다. 값은 Base64로 기록해 Go 문자열의 바이트를 보존하며 암호화는 아니다. 버킷·포인터는 기록하지 않고 복구 시 다시 만든다.

## 3. 함수별 역할

| 위치·함수 | 역할 |
|---|---|
| store.OpenPersistent | 파일 잠금 → 헤더·기록 검증 → 복구 → Sync 작업 시작 |
| store.Set / Delete | 변경 순서 잠금 → 검증된 변경을 AOF 기록 → 메모리 반영. 기록 실패면 변경하지 않음 |
| journal.sync | 현재 기록 순번과 파일을 잡은 뒤 락 밖에서 Sync. 완료한 순번만 확정 표시 |
| persistenceLoop | 주기 Sync, 필요 시 별도 작업으로 자동 재작성. 재작성 중에도 주기 Sync 지속 |
| Store.RewriteAOF | 메모리 상태 복사 → 임시 파일 작성 → 그동안의 변경분 반영 → Sync → 원자적 이름 교체 → 디렉터리 Sync |
| Store.PersistenceStatus | 현재 프로세스의 기록·Sync 순번, 파일 크기·재작성 횟수·오류 조회. 새 HTTP 관리 API는 만들지 않음 |
| Store.Close | 백그라운드 작업 종료 → 최종 Sync → 파일과 잠금 해제 |

실행 서버는 HTTP 종료와 Close를 합쳐 5초 기다린다. 운영체제 파일 I/O를 강제로 취소할 수 없으므로 제한을 넘으면 프로세스는 오류로 종료하고, 최종 Sync 성공을 주장하지 않는다.

재작성 시 스냅샷과 변경분 경계를 변경 순서 락으로 맞춘다. 기본 조건은 최소 4 MiB 이상에서 마지막 재작성 기준 크기의 두 배로 증가한 경우다. 파일 크기+삭제 예약량이 상한의 75% 이상이고 파일 증가량이 최소 재작성 크기 이상인 경우나 파일 상한에 도달한 경우도 시도한다. 파일 이름 교체는 같은 디렉터리에서 수행하며 실패 전에는 기존 파일이 기준이다. 재작성은 디스크와 별개로 데이터 구조를 순회하므로 지연이 완전히 사라지는 것은 아니다.

재작성에 성공해도 실제 공간을 회수하지 못했다면 1~60초 재시도 지연을 적용해 반복 전체 쓰기를 줄인다. 실제로 용량 부족으로 append가 거부된 뒤 DELETE가 발생했을 때만 다음 정리를 앞당긴다. 단순히 점유율이 75% 이상이라는 이유로 DELETE마다 재작성하지 않는다. 만료 키를 지연 삭제한 뒤 남은 보수적 예약량은 재작성에서 재계산한다. 예약 계산은 128 탐색 단위 Scan에서 합산하고, DEL JSON의 이스케이프 규칙과 프레임 크기로 길이를 직접 구하므로 전체 엔트리 슬라이스나 JSON 버퍼를 만들지 않는다. 전체 순회 비용 자체는 남는다. 이전 버전의 예약 없는 AOF는 시작 시 필요하면 재작성하고, 유효 데이터와 삭제 예산이 한도에 들어가지 않으면 원본을 보존하고 한도 증설을 요구한다.

### 손상 파일의 명시적 복구

0으로 채워진 끝부분도 자동으로 안전한 패딩이라고 판단하지 않는다. 서버를 멈춘 뒤 검사한다. 손상이 있으면 JSON 보고서를 출력하고 종료 코드 1을 반환하며 원본은 바꾸지 않는다.

```bash
go run ./cmd/aofrepair -input data/appendonly.aof
```

`valid_bytes`는 마지막 정상 기록 끝, `bytes`는 전체 길이, `zero_tail`은 남은 영역이 전부 0인지 여부다. 중간 손상이라면 마지막 정상 기록 뒤의 정상 기록들도 복구 결과에서 제외된다. 먼저 보고서와 버릴 범위를 검토한다.

명시적으로 손실을 허용할 때만 **다른 새 경로**에 정상 구간을 복사한다.

```bash
go run ./cmd/aofrepair -input data/appendonly.aof -output data/recovered.aof -allow-data-loss
```

원본·기존 출력 파일을 덮어쓰지 않는다. 원본과 출력의 OS 잠금을 사용하고, 임시 파일을 완성·Sync한 뒤 새 이름으로 공개한다. 검사·복구는 활성 서버 파일에 실행하지 않는다. 파일 고장을 원상 복원하거나 버려진 값을 되살리는 도구가 아니다.

복구 결과를 검증한 다음 새 파일로 서버를 시작한다. 원본은 그대로 보관한다.

```bash
MINI_REDIS_AOF_PATH=data/recovered.aof go run ./cmd/miniredis
```

## 4. 실행 설정

| 환경변수 | 기본값 |
|---|---|
| MINI_REDIS_AOF | true |
| MINI_REDIS_AOF_PATH | data/appendonly.aof |
| MINI_REDIS_AOF_MAX_BYTES | 268435456 |
| MINI_REDIS_AOF_REWRITE_MIN_BYTES | 4194304 |
| MINI_REDIS_DEBUG | false |

상한과 재작성 기준은 양수이며 재작성 기준이 상한보다 작아야 한다. 상대 경로는 실행 디렉터리 기준이다. 영속성 파일과 임시 파일의 내용을 직접 편집하지 않는다. 같은 데이터 파일은 서버 하나만 열 수 있다. data/는 Git에서 제외한다.

```bash
MINI_REDIS_DEBUG=true go run ./cmd/miniredis
```

시작 시 `AOF_REPLAY`, 변경 시 `AOF_APPEND`, 동기화 시 `AOF_SYNC`, 재작성 시 `AOF_REWRITE`를 볼 수 있다. 값 본문은 로그에 기록하지 않는다. 성능 측정에서는 debug를 끄고 영속성 활성 여부를 결과에 기록한다.

영속성 없이 비교하려면 `MINI_REDIS_AOF=false go run ./cmd/miniredis`로 실행한다. 이 모드의 변경은 기존 AOF에 반영되지 않는다. 나중에 다시 켜면 기존 파일 상태를 복구하므로 서로 다른 모드의 데이터를 같은 것으로 생각하지 않는다.

## 5. 직접 실행할 명령 5개

프로젝트 루트에서 실행한다. 각 테스트는 독립 임시 디렉터리만 사용한다.

① 값·삭제·TTL 복구:

```bash
go test ./internal/store -run '^TestAOFReplayValueDeleteAndTTL$' -count=1 -v
```

예상: `REPLAY: 갱신·삭제·빈 값·바이너리 복원, 중단 시간만큼 TTL 감소, 만료된 이전 값 부활 방지`, PASS.

② 손상 처리:

```bash
go test ./internal/store -run '^TestAOFTailRepairAndCorruption$' -count=1 -v
```

예상: 마지막 불완전 기록은 복구, 헤더·길이·본문 체크섬 손상은 시작 거부, PASS.

③ 기록 실패·자원 제한:

```bash
go test ./internal/store -run '^TestAOF(FailureAndSizeLimit|SyncDoesNotBlockReads)$' -count=1 -v
```

예상: 변경 거부·기존 조회 유지, Sync를 멈춰도 Get 완료, PASS.

④ 재작성과 동시 변경:

```bash
go test -race ./internal/store -run '^TestAOF(Rewrite|Background)' -count=1 -v
```

예상: 동시 저장·삭제 보존, 재작성 실패 시 원본 유지, 자동 Sync·파일 축소·단일 소유자 검사, PASS.

⑤ 일부러 깨뜨려보기 — 최초 TTL로 복원하면?

```bash
MINI_REDIS_EXPERIMENT=reset-ttl-on-replay go test ./internal/store -run '^TestExperimentResetTTLOnReplay$' -count=1 -v
```

예상: `의도한 실패: 최초 TTL로 재저장하면 남은 TTL이 100ms로 연장됨; 정상 복구는 40ms`, FAIL·종료 코드 1. 제품 코드는 바꾸지 않고 잘못된 복구를 테스트 안에서만 흉내 낸다. 환경변수는 해당 명령에만 적용된다.

## 6. 내부 로그

디버그 테스트는 `go test ./internal/store -run '^TestAOFDebugLogging$' -count=1 -v`로 실행한다.

```text
level=INFO msg=AOF_REPLAY bytes=8 keys=0 tail_repaired=false
level=DEBUG msg=AOF_APPEND op=set key=visible-key written=1 synced=0 bytes=...
level=DEBUG msg=AOF_SYNC written=1 synced=1 elapsed=...
level=INFO msg=AOF_REWRITE bytes=... keys=1
```

written은 현재 프로세스에서 추가 기록한 순번, synced는 Sync 완료를 확인한 순번이다. 성공 응답 직후에는 두 값이 다를 수 있다. 순번은 영구적인 전역 ID가 아니며 재시작 시 기준이 바뀐다.

## 7. 검증

필수 검증은 값 갱신·삭제·빈 값·최대 TTL 복구, 중단 중 TTL 경과, 만료 덮어쓰기 부활 방지, 꼬리 복구, 완전한 기록 손상 거부, 기록·Sync 실패, 크기·미동기화 지연 제한, Sync 중 조회, 재작성 중 동시 변경, 변경분 버퍼 초과·임시 파일 실패, 자동 Sync·재작성, 중복 프로세스 잠금, HTTP 503 변환이다.

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go build ./...
```

실제 서버 검증은 임시 파일·포트에서 1초 Sync와 자동 재작성을 확인한 뒤 SIGKILL → 재시작으로 최신 값·삭제·TTL을 확인한다. 부분 꼬리와 재작성 임시 파일을 추가해 복구·정리도 확인한다. 정상 SIGTERM 종료 코드 0을 확인하고 서버를 종료한다. 이 실험은 전원 차단·디스크 물리 고장을 재현한 것이 아니다.

2026-11-16 최종 결과: 전체 테스트·전체 레이스 검사·vet·build 모두 종료 코드 0. 별도 TTL 실패 실험은 예상대로 종료 코드 1. 실제 서버 SIGKILL 재시작 후 최신 값·삭제·만료·남은 TTL을 확인했다. 10,000ms TTL은 재시작 후 8,957ms로 줄었으며 검증용 파일은 재작성 전 2,409바이트에서 220바이트로 감소했다. 불완전 꼬리 복구·자기 임시 파일만 정리·무관 파일 보존을 확인했다. DEBUG=false 서버는 DEBUG 로그 0줄, SIGTERM 종료 코드 0이었다.

실행 환경은 Go 1.27.2 darwin/arm64. 검증 도구의 빌드 캐시는 `/private/tmp/mini-redis-go-build`를 사용했다. Mac 로컬 임시 HTTP 서버를 실행하기 위해 샌드박스의 로컬 네트워크 실행 권한을 사용했다.

### 검수 수정 후 회귀 검증

- TestMutationGateDeadlineAndCancellation: 락 대기 기한·취소, 미수락 요청의 지연 실행 방지.
- TestBlockedAppendBoundsCallerAndKeepsOrder: 파일 Write를 멈춰도 호출자는 기한 내 반환. 조회 유지, 실행 중인 변경은 기한 후 완료될 수 있음을 확인.
- TestRewriteCutoverDeadline: 교체 단계의 두 번째 Sync를 멈춘 상태에서도 SET·DELETE 대기는 종료, 이후 정상 재개.
- TestDeletionReserveAllowsDeletesWhenSetsAreFull: SET 한도 소진 후 모든 기존 키를 DEL 로그와 함께 삭제, 재시작 후 부활하지 않음.
- TestAOFInspectionAndRecoveryPreservesSource 및 cmd/aofrepair/TestRecoveryCLI: 0 꼬리·중간 손상·불완전 기록 검사, 명시 동의, 새 파일만 복구, 원본·기존 출력·활성 파일 보호.
- TestScanBoundedCollisionChain: 한 버킷에 1,000키가 몰려도 스캔 배치의 탐색 예산 유지.
- TestFullLiveDataRewriteBacksOff: 유효 데이터뿐이라 공간을 못 줄이는 재작성도 재시도 지연 적용.
- TestHTTPMutationCancellation: HTTP 요청 context가 Store까지 전달되고 미수락 취소가 데이터를 변경하지 않음.

```bash
go test ./internal/store -run 'Test(MutationGate|BlockedAppend|RewriteCutover)' -count=1 -v
go test ./internal/store -run 'Test(DeletionReserve|AOFInspection|FullLiveData)' -count=1 -v
go test ./cmd/aofrepair -count=1 -v
```

모두 PASS가 예상 결과다. 전체 테스트·전체 레이스 검사·vet·build도 통과했다. 실제 HTTP 서버에서 SET 한도 소진 후 9키를 삭제했고 정상 종료·재시작 결과를 확인했다. CLI 검사에서 zero_tail=true·종료 코드 1, 손실 동의 없는 복구 거부, 새 파일 복구 후 검사 성공, 원본 SHA-256 불변, 복구 파일로 서버 시작·기존 값 보존·삭제 키 부재·활성 파일 검사 거부·정상 SIGTERM 종료를 확인했다.

클라이언트 제한이 3초보다 짧거나 연결이 닫혔다면 503 대신 연결 오류를 볼 수 있다. 응답 쓰기 시간 제한이 핸들러·디스크 호출을 취소한다고 해석하지 않는다. 복사 배치를 나눴어도 GC·스케줄링·테이블 자체 조회 비용까지 지연 상한을 보장하는 것은 아니다.

다음 단계인 성능 측정·원본 API 구현은 이번 작업에서 진행하지 않는다.


### 2차 검수 반영

- 용량 75% 이상에서 SET·DELETE 16쌍을 반복하는 회귀 테스트로 매 삭제마다 재작성하지 않음을 검증한다. 최소 증가량·예약 포함 압박·재시도 지연을 함께 유지한다.
- 실행 중 결과 불명 오류는 `ErrOutcomeUnknown`이며 `errors.Is(err, ErrPersistence)`도 유지한다. HTTP 503 코드는 `PERSISTENCE_OUTCOME_UNKNOWN`으로 구분한다.
- 변경 고루틴의 panic은 `MUTATION_PANIC` 로그에 원인과 스택을 남기고 추가 변경을 거부한다. Delete의 메모리 잠금도 defer로 해제한다. panic으로 반영 여부가 불명확해질 수 있어 현재 요청은 결과 불명으로 응답한다.
- 검사 도구는 AOF 본문을 수정하지 않지만 동시 실행 방지를 위해 옆에 `.lock` 파일을 만들 수 있다. 불완전 꼬리도 검사에서는 damage found·종료 코드 1이며, EOF/UnexpectedEOF인 불완전 꼬리는 서버 시작 시 자동 복구 대상이다. 체크섬 손상·0 꼬리는 자동 복구 대상이 아니다.
- 향후 성능 측정에서는 AOF on/off별 변경 고루틴·채널 생성 비용과 키 수별 교체 시간도 측정한다. 이번 작업에서는 성능 수치를 추정하지 않는다.


2차 반영 후 검증: `go test ./... -count=1`, `go test -race ./... -count=1`, `go vet ./...`, `go build ./...`, `git diff --check` 모두 통과했다. 새 회귀 테스트 `TestHighOccupancyDeletesDoNotRewriteEveryTime`, `TestRewritePressureIncludesDeletionReserve`, `TestDeleteRecordLengthMatchesEncoding`, `TestMutationPanicLogsAndReleasesMemoryLock`은 레이스 검사로 5회 반복 통과했다. HTTP 오류 코드 구분은 `TestPersistenceOutcomeErrorCodes`에서 확인한다.
