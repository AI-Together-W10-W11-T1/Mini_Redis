# 검증 케이스 대응표

과제 요구사항과 [팀 PRD](learning/PRD.md) 6장의 검증 케이스가 어느 테스트로 확인되는지 정리했습니다. 전체 실행 명령은 다음과 같습니다.

```bash
test -z "$(gofmt -l .)" && go vet ./... && go test -race -count=1 ./...
```

마지막 확인: 2026-11-16, Go 1.24 linux/amd64에서 위 명령 전체 통과. GitHub Actions는 Ubuntu에서 Go 1.22와 최신 버전, macOS에서 최신 버전으로 같은 명령을 실행합니다. Go 1.22로 만든 레이스 테스트 바이너리는 최신 macOS 러너에서 실행 전에 중단되어(`missing LC_UUID load command`) macOS의 Go 1.22 조합은 제외했습니다.

## 과제 요구사항

| 요구사항 | 구현·근거 |
|---|---|
| 해시 테이블로 키-값 저장소 직접 구현 | `internal/hashtable`. 내장 map을 저장소로 쓰지 않음 |
| 외부에서 사용 가능 | HTTP API `PUT/GET/DELETE /keys/{key}`. [API 문서](api.md) |
| 설계 원리·구현 과정·주요 함수 설명 | [설계 선택](design-decisions.md), [구조](architecture.md), [단계별 학습 기록](learning/README.md) |
| 동시성 구조 | 전역 Mutex + 변경 순서 게이트. 아래 T3(Store 수준과 HTTP 전체 경로) |
| 만료된 값 요청 처리 | 만료 정각부터 404, 지연 삭제 + 주기 정리. 아래 U4~U6, U10 |
| API 형태 설계 | [API 문서](api.md), 오류 코드 표 |
| 무효화 방식 | 키별 DELETE와 TTL. 아래 F5 |
| 서버 다운 시 보존 (Optional) | AOF. 아래 PT1~4 |
| 저장·조회·삭제 단위 테스트 | 아래 T1 |
| 기능 테스트: 간단한 API를 만들어 응답을 저장해 활용 | 원본 API와 소비 앱 `cmd/demo`. [데모 문서](demo.md), 아래 T2 |
| 엣지 케이스 | 아래 U2~U9, F1, F6~F8 |
| 사용할 때와 안 할 때 성능 비교 | 측정 도구 `cmd/cachebench`. [측정 방법](benchmark.md), [결과](results/prd-t4.md) |

## T1. 단위 테스트

| ID | 케이스 | 테스트 |
|---|---|---|
| U1 | 저장·갱신·삭제와 개수(AC1~6) | `TestTableSetGetUpdateAndEmptyValue`, `TestStoreTTLTransitions`, `TestStoreMissingExpiredAndDelete`, `TestStoreValidationPreservesValueAndTTL` |
| U2 | 없는 키 조회·삭제, 대소문자 구분 | `TestTableSetGetUpdateAndEmptyValue`, `TestStoreMissingExpiredAndDelete`, `TestHTTPKeyRoundTrip`(키 `A`와 `a`) |
| U3 | 한 키만 갱신·삭제, 충돌 키 갱신 | `TestTableCollisionDeletePositions` |
| U4 | 무기한↔TTL, 더 짧은·더 긴 TTL로 교체 | `TestStoreTTLTransitions` |
| U5 | 만료 직전·정각·이후, 반복 조회, 만료 키 삭제 | `TestStoreTTLBoundaryAndRemaining`, `TestStoreMissingExpiredAndDelete` |
| U6 | 만료 후 재저장, 과거 정리 작업이 새 값을 지우지 않음 | `TestStoreConcurrentExpirationAndResize`, `TestSweepConcurrentMutationReservationInvariant`, `TestSweepDoesNotDeleteValueStoredAgain`, `TestReapToleratesMutationsBetweenBatches` |
| U7 | 실제 충돌에서 첫·중간·끝 삭제와 재삽입 | `TestTableCollisionDeletePositions`(실제 FNV 충돌과 해시 주입 모두), `TestScanBoundedCollisionChain`, `TestReapRemovesFromOneLongChain` |
| U8 | 확장·축소 전후 보존, 용량 거부와 삭제 후 재삽입 | `TestTableResizeThresholdAndPreservation`, `TestShrinkAfterMassDeletion`, `TestDataCapacityAccounting`, `TestCapacityFailureDoesNotAppendAndReplayCanShrink` |
| U9 | 잘못된 키·필드·타입·TTL, 최대값 경계, 매우 큰 TTL | `TestStoreValidationPreservesValueAndTTL`, `TestHTTPValidationAndErrors`, `TestHTTPGetRemainingTTL` |
| U10 | 조회되지 않는 만료 데이터 자동 회수 | `TestActiveExpirationMemoryAndClose`, `TestExpirationSustainedShortTTL`, `TestCapacityPressureReclaimsAndReservesReplacement`, `TestBucketArrayShrinksAfterExpiredBurst` |

## T2. 외부 API·캐싱 기능 테스트

F2~F8은 `cmd/demo/main_test.go`가 원본·mini-Redis·소비 앱을 각각 HTTP 서버로 띄워 PRD 경로 그대로 검증합니다. `internal/demo/demo_test.go`에는 같은 흐름을 먼저 확인했던 초기 테스트가 남아 있습니다.

| ID | 케이스 | 테스트 |
|---|---|---|
| F1 | HTTP로 AC1~6, 경계, 잘못된 입력, 없는 경로·메서드·Content-Type, 특수문자 왕복 | `TestHTTPKeyRoundTrip`, `TestHTTPValidationAndErrors`, `TestHTTPGetRemainingTTL` |
| F2 | 같은 상품 2회: MISS 후 HIT | `TestF2F3MissThenHitPerProduct` |
| F3 | 상품 A·B의 캐시 독립 | `TestF2F3MissThenHitPerProduct` |
| F4 | 원본만 변경: 만료 전 예전 값, 만료 후 새 값 | `TestF4OriginChangeAppearsAfterTTL` |
| F5 | 소비 앱으로 변경: TTL 전에 무효화 | `TestF5AppChangeInvalidatesBeforeTTL` |
| F6 | cache=off는 캐시를 읽지도 쓰지도 않음, 잘못된 옵션은 400 | `TestF6BypassLeavesCacheUntouched` |
| F7 | 없는 상품은 매번 404, 부재 미캐싱 | `TestF7MissingProductIsNotCached` |
| F8 | 원본 실패는 502·미캐싱, 복구 후 정상 | `TestF8OriginFailureIsNotCached` |
| 추가 | 캐시 장애 시 502, 거부된 변경은 캐시 유지, Bearer 토큰 | `TestCacheFailuresAndRejectedChange` |

F6은 mini-Redis로 가는 요청 수를 직접 세어 우회 요청이 캐시에 접근하지 않았음을 확인하고, 기존 캐시의 값과 남은 TTL이 그대로인지 비교합니다.

## T3. 동시성 테스트

| ID | 케이스 | 테스트 |
|---|---|---|
| C1 | 작업자별 다른 키 저장·조회·삭제 | `TestStoreConcurrentOperations` |
| C2 | 같은 키에 동시 저장·조회, 부분 갱신 없음 | `TestStoreConcurrentOperations` |
| C3 | 같은 키 저장·삭제·조회 경합 후 최종 상태 | `TestStoreConcurrentOperations` |
| C4 | 만료 감지→갱신, 정리·재해싱 중 요청 | `TestStoreConcurrentExpirationAndResize`, `TestSweepConcurrentMutationReservationInvariant` |

모든 테스트는 레이스 탐지기(`-race`)와 함께 실행합니다.

### HTTP 전체 경로 (기본 프로필)

`internal/httpapi/concurrency_test.go`가 실제 HTTP 서버를 띄워 C1~C3을 검증합니다. 작업자는 시작 장벽 뒤에서 동시에 출발하고, 5xx·연결 오류·예상 밖 응답은 모두 실패로 셉니다.

| ID | 테스트 | 판정 |
|---|---|---|
| C1 | `TestT3C1DistinctKeysOverHTTP` | 작업자별 키 저장·조회·3개 중 1개 삭제. 끝난 뒤 사용한 키 전체를 다시 조회해 삭제한 키는 404, 나머지는 자기 값인지 확인 |
| C2 | `TestT3C2SameKeyWritesOverHTTP` | 한 키에 작업자·순번이 들어간 약 2KB 값과 서로 다른 TTL을 동시 저장. 조회 값은 누군가 실제로 쓴 완전한 값이어야 하고, 최종값은 어느 작업자의 마지막 쓰기여야 함 |
| C3 | `TestT3C3SetDeleteGetRaceOverHTTP` | 한 키에 저장·삭제·조회 경합. 조회는 404이거나 실제로 쓴 값. 삭제 성공 횟수가 저장 횟수를 넘지 않음. 최종 저장 후 그 값, 최종 삭제 후 404, 재삭제는 0 |

판정은 엄격하게 합니다. 응답 본문은 정해진 필드만 가진 JSON 객체여야 하고(`key`·`value`·`ttl_ms`, `deleted`는 0 또는 1, `ok`는 true), 값은 정규 표기(`w<작업자>-i<순번>`, 부호·앞자리 0·뒤따르는 문자 없음)에 두 수가 모두 범위 안이며 다시 구성한 문자열과 정확히 같아야 합니다. C3은 여기에 더해 그 작업자·순번이 실제로 PUT을 수행하는 단계인지도 확인합니다.

판정 자체도 검증합니다. `TestT3ScenariosDetectFaultyServers`는 실제 핸들러의 응답 일부를 고의로 망가뜨리는 가짜 서버 28종(쓰지 않은 값, 음수·범위 초과·뒤에 붙은 문자, PUT 대상이 아닌 단계의 값, 잘리거나 섞인 값, 필드 누락·타입 오류·JSON 아님, 잘못된 삭제 개수, 500 응답 등)에 같은 시나리오를 돌려 **모두 실패로 판정되는지** 확인하고, 고장 없이 같은 경로를 거치면 통과하는지도 확인합니다. `TestT3ValueChecksAcceptOnlyWrittenValues`는 값 판정 함수의 수용·거부 목록을 직접 확인합니다.

기본 실행(`go test`)은 작업자 50개 × 100회이고, PRD 기본 프로필(작업자 50개 × 1,000회, 제한 60초)은 환경변수로 실행합니다.

```bash
MINI_REDIS_T3_FULL=1 go test ./internal/httpapi -run T3 -count=1 -v
```

2026-11-16, Linux 4코어·Go 1.24, 메모리 모드에서 기본 프로필을 실행한 결과입니다. 예상 밖 응답은 0건입니다.

| ID | HTTP 요청 수 | 소요 (일반) | 소요 (`-race`) |
|---|---|---|---|
| C1 | 166,700 | 7.1초 | 35.5초 |
| C2 | 100,001 | 6.1초 | 21.3초 |
| C3 | 50,005 | 1.2초 | 4.8초 |

요청 수는 경합 뒤의 확인 요청을 포함합니다(C1은 키 50,000개를 다시 조회). 팀 공용 스크립트 자체는 실행하지 않았고, 같은 프로필을 Go 테스트로 구현한 것입니다. C2의 값·TTL 조합과 C4의 순서 제어는 위 Store 수준 테스트가 담당합니다.

## 영속성 (PT1~4)

| ID | 케이스 | 테스트 |
|---|---|---|
| PT1 | 저장·덮어쓰기·삭제 후 재시작 | `TestAOFReplayValueDeleteAndTTL`, `TestAOFRewriteConcurrentChanges` |
| PT2 | 남은 수명만 복원, 중단 중 만료된 키는 부재 | `TestAOFReplayValueDeleteAndTTL` |
| PT3 | 쓰기·Sync 실패 주입 | `TestAOFFailureAndSizeLimit`, `TestHTTPPersistenceUnavailable`, `TestPersistenceOutcomeErrorCodes` |
| PT4 | 잘린 꼬리, 체크섬 손상, 재작성 중 실패 | `TestAOFTailRepairAndCorruption`, `TestAOFRewriteOverflowAndFailureKeepOriginal`, `TestAOFInspectionAndRecoveryPreservesSource` |

프로세스 종료·재시작만 검증했습니다. 전원·저장 장치 장애는 검증하지 않았습니다.

## 공용 스크립트를 쓸 때 알아 둘 동작

PRD의 공통 계약과 다르거나 해석이 갈릴 수 있는 부분입니다. 모두 의도한 동작이며 코드는 바꾸지 않았습니다.

| 동작 | PRD | 대응 |
|---|---|---|
| GET 응답에 `ttl_ms`가 함께 나옴: `{"key":"x","value":"a","ttl_ms":59999}` | `{"key":"x","value":"a"}` | `key`와 `value`는 계약과 같습니다. 본문 전체를 문자열로 비교하는 스크립트라면 필드별 비교로 바꿔야 합니다. 무기한 키는 `-1` |
| AOF가 기본으로 켜져 있음 | 테스트별 초기 상태 분리, T4는 영속성 비활성 | 공용 테스트와 성능 비교는 `MINI_REDIS_AOF=false`로 실행합니다. 켠 채로 재시작하면 `data/appendonly.aof`의 키가 복원됩니다 |
| Content-Type 헤더가 없는 PUT을 허용 | 부적합 Content-Type은 415 | `text/plain`처럼 잘못 지정한 경우는 415입니다. 생략만 허용합니다 |
| 알 수 없는 필드는 400 | 무시·거부는 개인 선택 | 판정 제외 대상입니다 |
| `/health`가 `{"status":"ok","persistence":"aof"}` | `{"status":"ok"}` | `status` 값은 같습니다. 원본 API와 소비 앱은 `{"status":"ok"}`만 반환합니다 |
| 토큰을 설정하면 키 API가 401 | 정의 없음 | 기본(루프백 주소, 토큰 없음)에서는 인증이 꺼져 있습니다 |
| 키에 임의의 UTF-8 허용, 제어 문자·`.`·`..`은 400 | 영문·숫자·`:`·`_`·`-` 최소 지원 | 최소 지원 범위를 포함합니다 |
