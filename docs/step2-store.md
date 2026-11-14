# 2단계: 저장 규칙·TTL·동시성 보호

이번 단계에서는 ② Store를 구현한다. ③ 해시 테이블은 그대로 사용하며 HTTP·실행 서버·소비 앱·성능 측정은 이후 단계다. 프로젝트 루트에서 Go 테스트를 실행해 동작을 관찰한다.

```text
테스트 → ② Store: 입력 검증 → 락 획득 → 현재 시각 확인 → 만료 판단·저장·삭제
                └→ ③ HashTable: 해시·버킷·연결·확장
```

## 1. 입력 검증·오류 기준

구현 전에 확인한 기준이다. Store는 HTTP에 의존하지 않고 `ErrInvalidArgument` 또는 `ErrLimitExceeded`를 반환한다. 이후 HTTP 계층에서 `errors.Is`로 구분해 상태 코드와 JSON으로 변환한다. 여러 입력이 동시에 잘못된 경우는 먼저 확인한 오류를 반환한다.

키 확인 순서는 **빈 키 → 256바이트 초과 → 점 키 → UTF-8 → 제어문자**다. 길이 확인은 O(1)이므로 매우 긴 키를 문자 단위로 훑기 전에 413으로 거부한다. 따라서 256바이트를 넘으면서 제어문자도 포함한 키는 400이 아니라 413이다. 그다음 값 크기, TTL 범위 순서로 확인한다.

| 입력 | HTTP 기준 | 이번 단계 처리 |
|---|---:|---|
| 빈 키·잘못된 UTF-8·제어문자·키 전체가 `.` 또는 `..` | 400 | ErrInvalidArgument |
| 키 256바이트 초과 | 413 | ErrLimitExceeded |
| 값 65,536바이트 초과 | 413 | ErrLimitExceeded |
| TTL 0·음수·9,223,372,036,854ms 초과 | 400 | ErrInvalidArgument |
| TTL 문자열·비정수·null | 400 | HTTP JSON 해석 단계에서 거부 예정. Store 입력 타입은 이미 int64 |

정확히 상한 크기는 허용하며 글자 수가 아닌 UTF-8 바이트 수로 확인한다. 키의 공백·대소문자·인코딩 문자열을 임의로 바꾸지 않는다. 잘못된 입력은 기존 값과 만료 시각을 변경하거나 만료 항목을 정리하지 않는다. 요청 본문 크기·Content-Type·JSON 검증은 HTTP 단계에서 구현한다.

## 2. 함수별로 이해할 핵심

| 함수·구조 | 역할과 로직 | 꼭 알아야 할 점 |
|---|---|---|
| New | 시계·로거·자기 소유 해시 테이블을 연결 | nil 시계는 실제 time.Now를 사용. 테스트는 가짜 시계 주입 |
| Clock.Now | 현재 시각 제공 | 가짜 시계를 전진시키면 실제 대기 없이 만료 경계 재현 가능 |
| Set | 입력 검증 → 락 → 시간 확인 → 값·만료 시각을 함께 저장 | TTL 생략(nil)은 무기한이며 이전 TTL 제거. TTL은 락 대기 후부터 계산 |
| Get | 키 검증 → 락 → 동일한 현재 시각으로 값·만료 판단 | 조회로 TTL을 연장하지 않음 |
| Delete | 키 검증 → 락 → 상태 확인 → 유효 항목 삭제 | 만료 항목도 물리 정리하지만 Deleted=false |
| lookupLocked | 없음·무기한·만료·유효 TTL 구분, 만료 항목 물리 삭제 | 반드시 락을 가진 상태에서만 호출 |
| validateKey | 빈 값·바이트 한도·점 키·UTF-8·제어문자 순서로 확인 | 저장뿐 아니라 조회·삭제에도 같은 규칙 적용. O(1) 길이 확인을 먼저 |
| State.String | Missing·Live·Expired를 로그에 읽기 쉽게 표시 | 외부 HTTP 응답과는 별개의 내부 상태 |

`internal/store/store.go`에 구현, `internal/store/store_test.go`에 테스트가 있다. 호출자는 테이블을 직접 다루지 않고 같은 Store 인스턴스를 공유한다. Store를 값으로 복사하지 않는다.

### 만료 판단

| 상황 | State | TTLMillis | 값 |
|---|---|---:|---|
| 물리 항목 없음 | Missing | -2 | 반환하지 않음 |
| 무기한 | Live | -1 | 반환 |
| 만료 전 | Live | 남은 밀리초 올림, 최소 1 | 반환 |
| 현재 시각이 만료 시각 이상 | Expired | -2 | 반환하지 않고 물리 정리 |

정리한 키를 다시 조회하면 Missing이다. Expired는 과거 이력을 계속 보관한다는 뜻이 아니다. -2는 내부 표시이며 HTTP의 404에는 노출하지 않는다. time.Time을 그대로 사용해 실제 시계의 단조 시계 성분을 보존한다.

남은 시간이 0.4ms면 1ms로 반환한다. 큰 TTL에서도 안전하도록 나눗셈 후 나머지가 있을 때만 1을 더한다. 만료 시각과 정확히 같으면 만료다.

### 동시성 보호

하나의 `sync.Mutex`가 같은 Store의 모든 해시 테이블 접근을 보호한다. 조회도 만료 항목을 삭제할 수 있으므로 락을 사용한다. 만료 확인과 삭제 사이에 다른 요청이 새 값을 넣어 버리는 상황을 막기 위해 두 작업을 같은 락 안에서 끝낸다. 해시 테이블의 리사이징도 이 범위에 포함된다.

이 방식은 설명·검증이 단순하지만 같은 Store의 연산은 한 번에 하나씩 처리한다. 이 단계만으로 실무 성능을 보장하지 않으며 외부 성능 비교는 이후 단계다. 만료는 요청 시에만 정리하므로 다시 접근하지 않는 만료 키는 메모리에 남을 수 있다.

## 2-1. 알고리즘 선택 근거와 대안 비교

### 동시성: 왜 단일 Mutex인가

| 방식 | 동작 | 장점 | 이 프로젝트에서의 문제 |
|---|---|---|---|
| **단일 Mutex (채택)** | 저장·조회·삭제·만료 정리·리사이징 전체를 한 락으로 직렬화 | 확인→삭제가 항상 한 덩어리. 설명·테스트가 가장 쉬움 | 같은 Store 연산은 동시에 하나만 실행 |
| RWMutex | 조회는 읽기 락, 저장·삭제는 쓰기 락 | 읽기만 하는 연산은 병렬 가능 | Get이 만료 항목을 **삭제**하므로 읽기 전용이 아니다. 읽기 락으로 확인 → 쓰기 락으로 승격 → **재확인**이 필요하고, 재확인을 빠뜨리면 새 값이 지워진다 |
| 샤딩(스트라이프) 락 | 키 해시로 N개 조각을 고르고 조각마다 락·테이블 | 다른 키 연산이 여러 코어에서 병렬 | 테이블 N개·리사이징 N번. 1단계의 16→32 확장 설명과 테스트가 조각별로 흩어짐. 전체 키 수 같은 연산은 모든 락 필요 |
| 단일 고루틴(액터·채널) | 한 고루틴만 테이블을 만지고 요청은 채널로 전달 | Redis 이벤트 루프와 비슷, 락 없음 | 요청마다 채널 왕복이 Mutex보다 느리고 응답 채널·종료 처리 코드가 늘어남 |

`TestStoreConcurrentExpirationAndResize/split_check_and_delete_loses_new_value`가 RWMutex식 "확인 따로, 삭제 따로"에서 새 값이 사라지는 순서를 그대로 재현한다.

**측정 근거(2026-11-14, Linux 4 vCPU 컨테이너, Go 1.24):** 락 안의 연산 1회는 단일 Mutex 약 0.13µs, RWMutex 0.11~0.14µs, 16샤드 0.12~0.14µs로 차이가 거의 없었다. 같은 환경에서 아무 일도 안 하는 HTTP 요청 1건의 왕복은 약 12.5µs였다. 락 구간이 HTTP 요청 비용의 약 1%이므로, 락 방식을 바꿔도 전체 처리량은 거의 변하지 않는다. 그래서 가장 단순한 단일 Mutex를 유지한다. 직접 확인하려면 아래 벤치마크를 실행한다(머신마다 숫자는 다르다).

```bash
go test ./internal/store -run '^$' -bench StoreParallel -cpu 1,4 -count=1
```

샤딩이 의미 있어지는 시점은 HTTP를 빼고 라이브러리처럼 초당 수백만 건을 직접 호출하거나, 디버그 로그처럼 락 안에서 I/O를 하는 경우다. 디버그 로그는 락 안에서 stderr에 쓰므로 켜면 모든 요청이 느려진다. 성능 측정에서는 반드시 끈다.

### 만료: 왜 지금은 지연 만료(lazy)만인가

| 방식 | 동작 | 장점 | 단점 |
|---|---|---|---|
| **지연 만료 (채택)** | 조회·삭제 시점에 만료 확인 후 정리 | 추가 구조·고루틴 없음. 정확성(만료 값 미반환)을 100% 보장 | 다시 접근하지 않는 만료 키는 메모리에 남음 |
| 지연 + 주기 샘플링 (Redis 방식) | 100ms마다 일부 키를 골라 만료 키를 정리, 만료 비율이 높으면 반복 | 메모리 회수. 한 번에 일정량만 검사해 지연이 짧음 | 백그라운드 고루틴·종료 처리 필요. 해시 테이블에 버킷 순회 API 추가 |
| 만료 힙(우선순위 큐) | 만료 시각 순으로 정렬해 가장 이른 것부터 삭제 | 만료 직후 정확히 회수 | Set마다 O(log n). 덮어쓴 키의 옛 힙 항목 처리(세대 번호) 필요. 메모리 2배 |
| 키별 타이머(time.AfterFunc) | 키마다 타이머 등록 | 개념이 직관적 | 키 수만큼 타이머. 덮어쓴 뒤 옛 타이머가 새 값을 지우는 경합(PRD U6·C4) 방지 코드 필요 |

정확성(만료 값을 절대 반환하지 않음)은 지연 만료만으로 이미 보장된다. 다른 방식은 모두 **메모리 회수**를 위한 추가 장치이며, 설계 문서가 "별도 정리 작업·타이머는 만들지 않는다"로 승인한 범위 밖이다. PRD에서는 미조회 만료 데이터 자동 회수를 P1 권장으로 둔다. 추가한다면 해시 테이블 구조를 그대로 쓰는 **버킷 커서 기반 점진 스윕**(호출 1회에 버킷 N개만 검사, 테스트는 가짜 시계로 직접 호출, 서버에서만 100ms 주기로 실행)이 가장 단순하다.

### TTL 올림 계산

남은 나노초를 밀리초로 나눈 뒤 나머지가 있으면 1을 더한다. `(남은 시간 + 999,999ns) / 1ms` 방식은 최대 TTL 근처에서 int64 오버플로가 날 수 있어 쓰지 않는다. 올림이므로 0.4ms 남은 키가 0을 보고하지 않고, `>=` 비교로 정각을 만료로 보므로 "TTL 1인데 다음 순간 404"는 정상 동작이다.

## 3. 실행 명령과 예상 출력

```bash
go test ./... -count=1
go vet ./...
go build ./...
```

예상 출력: hashtable·store는 `ok`, 나머지 빈 패키지는 `[no test files]`. vet·build는 성공 시 출력 없이 종료 코드 0이다.

```text
ok  mini-redis/internal/hashtable  ...s
ok  mini-redis/internal/store      ...s
```

## 4. 직접 실행할 명령 5개

각 명령은 새 Store를 사용하며 실제 데이터를 기다리지 않고 가짜 시계를 움직인다. `-count=1`은 재실행, `-v`는 설명 출력이다. 환경변수는 테스트가 읽어 로거로 전달하며 Store가 직접 읽지 않는다.

### ① 만료 직전·정각·정리 후

```bash
MINI_REDIS_DEBUG=true go test ./internal/store -run '^TestStoreTTLBoundaryAndRemaining$' -count=1 -v
```

2ms TTL로 저장하고 가짜 시계를 전진시킨다. 같은 시각에 다시 조회해도 TTL이 늘지 않는다.

```text
BEFORE: 0.4ms 남음 → Live, ttl_ms=1 (올림, 조회로 연장 없음)
AT: 정확히 만료 → Expired, 물리 항목 삭제
AFTER: 정리 후 다시 조회 → Missing
PASS
```

### ② TTL 교체와 제거

```bash
MINI_REDIS_DEBUG=true go test ./internal/store -run '^TestStoreTTLTransitions$' -count=1 -v
```

무기한에서 TTL을 설정하고, 연장·단축한 뒤 TTL을 생략해 다시 무기한으로 바꾼다. 만료된 키를 재저장하는 경우도 확인한다.

```text
TRANSITIONS: 무기한 → TTL → 연장 → 단축 → 무기한(빈 값), 이전 TTL 제거
RENEW: 만료 전후 재저장 모두 새 값·새 만료 정책 적용
PASS
```

### ③ 잘못된 요청이 기존 데이터를 보존하는지

```bash
MINI_REDIS_DEBUG=false go test ./internal/store -run '^TestStoreValidationPreservesValueAndTTL$' -count=1 -v
```

키·값 바이트 경계와 잘못된 TTL을 확인한다. 10ms TTL 중 3ms가 지난 값을 잘못된 입력으로 덮어쓰려 해도 기존 값과 남은 7ms를 유지해야 한다. 최대 TTL의 변환과 올림 계산도 검증한다.

```text
VALIDATION: 키·값 바이트 경계 및 TTL 범위 확인, 오류 시 기존 값·만료 시각 보존
PASS
```

### ④ 동시에 저장·조회·삭제해 보기

```bash
MINI_REDIS_DEBUG=false go test -race ./internal/store -run '^TestStoreConcurrentOperations$' -count=1 -v
```

16개의 고루틴(동시에 실행되는 작업)이 각자의 키와 같은 공유 키를 저장·조회·삭제한다. 공유 키는 TTL 값과 무기한 값을 번갈아 저장하며, 조회 결과의 값·TTL이 서로 다른 저장 작업의 것으로 섞이지 않아야 한다. 작업 종료 후 모든 개별 키를 다시 검증한다. 실패하거나 교착되면 테스트가 실패한다. 이 테스트는 출력량을 줄이기 위해 로거를 항상 끈다.

```text
CONCURRENT: 16명 동시 저장·조회·삭제, 1281키 보존, 값·TTL 혼합 없음, capacity=2048
PASS
```

### ⑤ 일부러 깨뜨려보기: 만료 판단을 생략하면?

```bash
MINI_REDIS_DEBUG=true MINI_REDIS_EXPERIMENT=skip-expiration-check go test ./internal/store -run '^TestExperimentWithoutExpirationCheck$' -count=1 -v
```

테스트 안에서만 잘못된 조회를 흉내 낸다. 만료 시각이 지났는데 해시 테이블에서 바로 읽으면 예전 값이 나온다. 같은 상태에서 정상 Store.Get은 Expired를 반환하고 물리 항목을 정리한다. 제품 코드와 락은 제거하지 않는다.

```text
의도한 실패: 만료 판단 생략 → 만료된 session을 "secret"로 반환; 정상 Get은 Expired
--- FAIL: TestExperimentWithoutExpirationCheck
FAIL
```

**이 명령만 종료 코드 1이 기대 결과다.** 일반 테스트에서는 SKIP된다. 일회성 환경변수로 실행하므로 제품 코드 복구 없이 `go test ./... -count=1`로 정상 테스트를 실행할 수 있다.

## 5. 내부 로그 읽기

실제로 확인한 만료 로그다. 같은 로거로 해시값·버킷·충돌·확장 로그도 출력된다.

```text
level=DEBUG msg=EXPIRATION op=get key=user:101 state=Live unlimited=false ttl_ms=2 cleaned=false
level=DEBUG msg=EXPIRATION op=get key=user:101 state=Live unlimited=false ttl_ms=1 cleaned=false
level=DEBUG msg=EXPIRATION op=get key=user:101 state=Expired unlimited=false ttl_ms=-2 cleaned=true
level=DEBUG msg=EXPIRATION op=get key=user:101 state=Missing unlimited=false ttl_ms=-2 cleaned=false
```

- state: 이번 요청에서 발견한 유효·만료·없음 상태.
- unlimited: 유효한 무기한 항목인지 여부.
- ttl_ms: 남은 밀리초, 무기한 -1, 부재·만료 -2.
- cleaned: 이번 요청에서 만료 항목을 물리적으로 삭제했는지 여부.
- STORE_DELETE의 deleted: 유효 키 삭제 여부. 만료 정리는 false.

Store도 생성 시 디버그 활성 여부를 고정하고 로그 호출 전에 조건을 확인한다. 비활성 시 로그 인자 준비를 생략한다. 전체 값은 로그에 넣지 않는다. 성능 비교에서는 끈다.

## 6. 테스트 코드와 검증 결과

| 필수 테스트 | 확인하는 내용 |
|---|---|
| TestStoreTTLBoundaryAndRemaining | 직전·정각·이후, 최소 1ms 올림, 조회 비연장, 물리 정리 |
| TestStoreTTLTransitions | 무기한↔TTL, 연장·단축, 빈 값, 만료 전후 재저장 |
| TestStoreMissingExpiredAndDelete | 없음·만료 내부 구분, 삭제 결과, 반복 삭제, 재등록 |
| TestStoreValidationPreservesValueAndTTL | UTF-8·제어문자·바이트 한도·TTL 범위, 최대 TTL, 오류 시 상태 보존 |
| TestStoreConcurrentOperations | 같은 키·다른 키 동시 연산, 리사이징, 값·TTL 조합, 최종 상태, 레이스 탐지 |
| TestStoreConcurrentExpirationAndResize (선택) | 확인·삭제를 나누면 새 값이 사라지는 순서 재현, 만료 정리·덮어쓰기·확장 동시 실행 후 새 값 보존 |

가짜 시계도 자체 락으로 보호한다. 만료 판정 테스트에 실제 sleep을 사용하지 않는다. 동시성 테스트의 10초 타이머는 교착 감지용이며 TTL 판정용이 아니다. 선택 테스트 중 만료·확장 복합 경합(TestStoreConcurrentExpirationAndResize)은 단일 락을 선택한 근거를 보여주므로 추가했다. 나머지 선택 테스트는 추가하지 않는다. BenchmarkStoreParallel은 판정용이 아니라 측정용이다.

검증 명령:

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go build ./...
MINI_REDIS_DEBUG=true go test ./internal/store -count=1 -v
MINI_REDIS_DEBUG=false go test ./internal/store -count=1 -v
```

2026-11-14 실행 결과: 모두 종료 코드 0. 레이스 탐지 보고 없음. DEBUG on에서 만료·해시 로그 확인, off에서 DEBUG 로그 0줄. 별도 실패 실험은 의도대로 종료 코드 1. 환경은 Go 1.27.2 darwin/arm64이며 검증 도구의 빌드 캐시는 `/private/tmp/mini-redis-go-build`를 사용했다.

사용자가 “다음”이라고 요청하기 전 HTTP 단계로 진행하지 않는다.
