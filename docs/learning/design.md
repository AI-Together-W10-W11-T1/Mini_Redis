# Mini Redis 구현 설계

2026-11-16 사용자 요청으로 D7을 AOF + 1초 주기 Sync로 변경했다. 상세 계약은 `docs/step4-persistence.md`를 따른다.

기준은 인터뷰 종료 후 확정한 **0단계 D1~D11 결정표**와 이후 사용자가 명시한 수정 사항이다. 로컬 Go 최소 1.22, 성능 측정 3초·10초·1회를 기준으로 한다. **설정 가능한 디버그 로그, GET의 남은 TTL 반환, 단계별 실행·학습 실험·테스트 제공**을 함께 반영한다. 이 문서의 시그니처는 계층 간 인터페이스 계약이다.

## 1. 디렉터리 구조·책임·의존 방향

아래 구조와 역할을 따른다.

```text
docs/design.md                  설계·계약·승인 기준
cmd/miniredis/                  서버 시작, 설정 읽기, 의존성 조립
cmd/aofrepair/                  정지한 AOF 검사·정상 구간을 새 파일로 복구
cmd/origin/                     성능 비교용 느린 원본 API 실행
cmd/demo/                       캐시 사용·미사용을 선택하는 소비 앱 실행
cmd/bench/                      부하 실행·통계 집계·CSV 출력 진입점
internal/httpapi/               ① 요청 처리: HTTP·JSON·응답 변환
internal/store/                 ② 저장 규칙: 입력 규칙·전역 락·TTL·무효화
internal/hashtable/             ③ 해시 테이블: 버킷·연결·탐색·삭제·확장
internal/config/                실행 설정 해석·검증
internal/origin/                원본 API의 상품 응답·지연·호출 계측
internal/demo/                  원본과 mini-Redis를 호출하는 소비 앱
internal/bench/                 준비·측정·원본 호출 수 대조·CSV 행 생성
internal/*/*_test.go            계층별 테스트는 해당 패키지 안에 배치
internal/demo/demo_test.go      httptest.NewServer 세 개를 연결한 외부 캐시 테스트
docs/results/                  성능 측정 CSV·실행 환경·결과 해석
```

| 구분 | 책임 | 의존 제한 |
|---|---|---|
| ① httpapi | 경로·메서드·JSON 해석, 응답·오류 변환 | store를 호출. hashtable 직접 접근 금지 |
| ② store | 입력 검증, 메모리 락·변경 순서 락, TTL·삭제·AOF | hashtable과 자신이 정의한 Clock 사용. HTTP 의존 금지 |
| ③ hashtable | FNV-1a 64비트, 체이닝, 키 비교, 개수·확장 | 표준 라이브러리만 사용. store·HTTP 역참조와 TTL 판단 금지 |
| cmd·config | 설정 읽기와 프로그램 시작·조립 | 실행 진입점의 조립 역할. 요청 처리 중 계층 우회 금지 |
| origin·demo·bench·테스트 | 외부 사용·성능 실험 | demo는 mini-Redis를, bench는 소비 앱·원본 /stats를 HTTP로 호출. CSV 출력은 cmd/bench가 담당하며 코어 직접 호출로 외부 검증을 대체하지 않음 |

요청 경로는 **① → ② → ③ 한 방향**이다. 로깅은 생성 시 전달한 표준 로거를 사용한다. Store의 `mu`가 모든 테이블 접근·지연 만료·재해싱을 보호하며 `writeMu`가 변경과 AOF 기록·교체·종료의 순서를 보장한다. 파일 I/O·Sync와 HTTP 본문 읽기·응답 쓰기는 메모리 락 밖에서 수행한다.

해시 테이블·저장 규칙·HTTP·설정·측정 집계 테스트는 각각의 패키지 안에 둔다. 외부 캐시 기능 테스트는 `internal/demo/demo_test.go`에서 세 테스트 서버를 연결한다. 별도 최상위 테스트 디렉터리는 만들지 않고 성능 결과는 `docs/results/`에 둔다.

## 2. 계층별 함수 시그니처와 결과 의미

이름·입출력 계약은 아래로 고정한다.

### ③ 해시 테이블

| 시그니처 | 계약 |
|---|---|
| `Hash(key string) uint64` | FNV-1a 64비트 직접 구현. UTF-8 바이트마다 XOR → 소수 곱셈 순서로 계산 |
| `New(logger *slog.Logger) *Table` | 초기 버킷 16개. nil·로그 비활성 로거 전달 가능. 생성 시 디버그 활성 여부를 확정 |
| `(*Table).Put(key string, record Record) bool` | 기존 키 교체면 true, 새 키면 false. 같은 키 중복 생성 금지 |
| `(*Table).Get(key string) (Record, bool)` | 물리 항목의 값 복사본과 존재 여부. 만료 여부는 판단하지 않음 |
| `(*Table).Delete(key string) bool` | 물리 항목 삭제 여부. 없으면 false·개수 불변 |
| `(*Table).Len() int` | 아직 정리하지 않은 만료 항목도 포함한 물리 키 수 |
| `(*Table).Capacity() int` | 현재 버킷 수 |
| `(*Table).Range(visit func(string, Record) bool)` | 레코드 복사본 순회. 호출자가 접근을 잠그며 콜백 안에서 테이블 변경 금지 |
| `(*Table).Scan(cursor *ScanCursor, budget int, visit func(string, Record)) bool` | 버킷 탐색+노드 방문을 예산만큼 수행. 전체 스캔 중 삽입·갱신·확장은 금지하며 배치 사이 만료 삭제만 허용 |
| `(*Table).resize(newCapacity int)` | 비공개 함수. 값·만료 시각·키 수를 보존해 재배치 |

새 키 삽입으로 적재율이 0.75를 초과하면 2배 일괄 확장한다. 기존 키 갱신으로 확장하거나 키 수를 늘리지 않는다. 해시값이 같아도 실제 키를 비교한다. 내부 Entry·next 포인터는 외부에 반환하지 않는다. 동시성 보호는 호출자인 store의 책임이다.

### ② 저장 규칙과 시계

| 시그니처 | 계약 |
|---|---|
| `Clock.Now() time.Time` | store에 정의하는 시계 인터페이스의 유일한 메서드. 실제 시계·가짜 시계 교체 가능 |
| `New(clock Clock, logger *slog.Logger) *Store` | Store가 자기 Table·메모리 락·변경 순서 락을 소유. 같은 인스턴스를 모든 요청에 공유 |
| `OpenPersistent(clock Clock, logger *slog.Logger, opts AOFOptions) (*Store, error)` | 파일 잠금·복구 후 주기 Sync 시작. 기존 New는 메모리 전용 |
| `(*Store).Close() error` | 백그라운드 작업 종료·최종 Sync·파일 잠금 해제 |
| `(*Store).RewriteAOF() error` | 유효 항목과 동시 변경분을 새 파일로 기록 후 교체 |
| `(*Store).PersistenceStatus() AOFStatus` | 파일 크기·기록/Sync 순번·재작성·오류. HTTP 관리 API 없음 |
| `(*Store).Set(key string, value string, ttlMillis *int64) error` | nil이면 무기한. 입력 오류는 변경 전 거부. 값·만료 시각 함께 교체 |
| `(*Store).SetContext(ctx context.Context, key string, value string, ttlMillis *int64) error` | 요청 취소·기한 전달. 영속성 변경 결과 대기는 기본 3초. 시작한 I/O는 기한 후 완료될 수 있음 |
| `(*Store).Get(key string) (LookupResult, error)` | 입력 오류만 error. 유효·없음·만료는 LookupResult.State로 구분 |
| `(*Store).Delete(key string) (DeleteResult, error)` | 유효 키면 Deleted=true. 없음·만료면 false. 발견한 만료 항목은 물리 삭제 |
| `(*Store).DeleteContext(ctx context.Context, key string) (DeleteResult, error)` | 취소·대기 제한 적용. 결과 불명 시 error이며 자동 재시도 금지 |

| 결과 타입의 필드 | 의미 |
|---|---|
| `LookupResult.State` | `Live`(유효), `Missing`(물리적으로 없음), `Expired`(이번 연산에서 만료 발견) |
| `LookupResult.Value` | Live일 때의 문자열. 빈 문자열도 유효한 값 |
| `LookupResult.TTLMillis` | Live는 -1 또는 양수. Missing·Expired는 내부 표시 -2. 4절의 HTTP 404에는 이 필드를 노출하지 않음 |
| `DeleteResult.State` | 삭제 직전의 Live·Missing·Expired 상태. 디버그·테스트용 |
| `DeleteResult.Deleted` | Live 항목을 삭제했을 때만 true |

**없음·만료 구분:** 내부에서는 로그·테스트를 위해 구분하지만 HTTP 조회는 같은 404다. 만료 정리 후 다시 조회하면 Missing이므로 과거 만료 이력을 계속 저장하지 않는다.

입력 규칙 검증은 공유 상태 변경 전에 수행한다. 유효 입력은 락 획득 후 Clock.Now를 읽고 판단하며 영속성 모드는 AOF 추가 기록 성공 후 메모리를 변경한다. SET은 값·TTL을 함께 교체한다. TTL용 주기적 정리 작업은 만들지 않는다. AOF는 별도 주기 Sync·재작성 작업을 사용한다. 가짜 시계는 테스트에서 시간을 전진시키며 동시 접근도 보호한다.

### ① 요청 처리

| 시그니처 | 계약 |
|---|---|
| `NewHandler(store *store.Store, logger *slog.Logger) http.Handler` | 같은 Store를 사용하는 HTTP 핸들러 생성 |
| `NewHandlerWithMaxBodyBytes(store *store.Store, logger *slog.Logger, limit int64) http.Handler` | 기본 생성자와 같은 API 동작에 양의 본문 크기 설정 적용. 기본 생성자는 512 KiB 사용 |
| `(*Handler).ServeKey(w http.ResponseWriter, r *http.Request)` | `/keys/{key}` 하나에 등록. 메서드를 직접 분기해 Set·Get·Delete 호출, 나머지는 JSON 405 |
| `(*Handler).Set(w http.ResponseWriter, r *http.Request)` | 키·JSON 해석 → store.Set → JSON 응답 |
| `(*Handler).Get(w http.ResponseWriter, r *http.Request)` | store.Get의 Live는 200, Missing·Expired는 404 |
| `(*Handler).Delete(w http.ResponseWriter, r *http.Request)` | store.Delete 결과를 deleted 1/0으로 변환 |

Clock·해시·기록 구조는 HTTP에 노출하지 않는다. JSON의 ttl_ms 생략과 null은 파싱 단계에서 구분한다. 생략만 Set의 nil로 전달하며 null은 400으로 거부한다.

## 3. 엔트리와 만료 표현

| 구조·필드 | 타입·의미 |
|---|---|
| Table.buckets | Entry 참조 배열. 길이가 버킷 수 |
| Table.size | 물리 항목 개수. 갱신·재해싱 시 불변 |
| Entry.key | string. UTF-8, 대소문자 구분 |
| Entry.hash | uint64. 키의 FNV-1a 해시값 캐시 |
| Entry.record | Record. 값·만료 시각을 한 번에 교체 |
| Entry.next | 다음 Entry 참조. 충돌 항목 연결 |
| Record.Value | string. 빈 문자열 허용, 최대 65,536바이트 |
| Record.ExpiresAt | time.Time. 영값은 만료 없음, 그 밖에는 만료 시각 |

실제 Clock은 Go의 time.Now를 사용하고 TTL은 해당 시각의 Add로 계산해 단조 시계 성분을 보존한다. 실행 중 Unix 밀리초로 바꿨다가 복원하지 않는다. 만료 없음은 IsZero로, 만료는 `현재 시각 >= ExpiresAt`으로 판정한다. 테스트는 주입한 가짜 시간원으로 경계 직전·정각·이후를 재현한다.

GET의 남은 TTL은 **락 안에서 읽은 동일한 현재 시각**으로 값과 함께 계산한다. 무기한은 -1, 유효 TTL은 남은 기간을 밀리초로 올림해 최소 1을 반환한다. 예: 0.4ms 남았으면 1, 정확히 만료됐으면 404. 올림 계산 자체의 정수 오버플로도 피한다. 응답 수신 시점까지 TTL이 그대로 유지된다는 뜻은 아니다.

TTL은 양의 정수 밀리초이며 Go time.Duration으로 안전하게 변환 가능한 최대값 **9,223,372,036,854ms**까지만 수용한다. 범위 확인 후 나노초로 변환하고 JSON 숫자를 float64로 먼저 반올림하지 않는다. AOF에는 절대 만료 시각을 기록하며 재시작 시 남은 기간을 현재 단조 시계에 연결한다. 중단 중 만료된 값은 복원하지 않는다.

## 4. HTTP API·키 위치·JSON 예시

키는 **본문이 아닌 `/keys/{key}` 경로의 한 구간**에 둔다. SET 본문에는 value·선택 ttl_ms만 둔다. API 세 개를 고정하고 EXPIRE·TTL 조회·INCR·패턴 삭제는 추가하지 않는다.

Go 1.22 이상의 ServeMux에 **메서드 접두사 없는 `/keys/{key}` 한 패턴**을 등록하고 핸들러가 GET·PUT·DELETE를 직접 분기한다. `/keys`와 정확한 `/keys/`도 별도 등록해 메서드와 무관하게 JSON 400으로 처리한다. `/keys/`의 등록 패턴은 하위 경로 전체가 아니라 정확한 경로만 받는 `/keys/{$}`를 사용한다. 루트의 나머지 요청은 JSON 404로 처리한다.

지원하지 않는 메서드(HEAD·OPTIONS 포함)는 JSON 405와 **`Allow: GET, PUT, DELETE`**를 반환한다. 라우터가 자동 생성한 HTML·텍스트 오류로 대체하지 않는다. 경로 정리로 리다이렉트되기 전에 아래 점 키 검증을 수행한다.

| 메서드·경로 | 본문 | 성공 응답 |
|---|---|---|
| PUT `/keys/{key}` | value 필수·ttl_ms 선택 | 200 `{"ok":true}` |
| GET `/keys/{key}` | 없음 | 200 `{"key":"user:101","value":"Minji","ttl_ms":29742}` |
| DELETE `/keys/{key}` | 없음 | 200 `{"deleted":1}` 또는 `{"deleted":0}` |

### 요청·응답 예시

- 유기한 저장: PUT `/keys/user%3A101`, 본문 `{"value":"Minji","ttl_ms":30000}` → 200 `{"ok":true}`.
- 이후 조회: GET `/keys/user%3A101` → 200 `{"key":"user:101","value":"Minji","ttl_ms":29742}`. 남은 시간 숫자는 조회 시점에 따라 변한다.
- 무기한 저장: PUT `/keys/user%3A101`, 본문 `{"value":"Alex"}` → 200 `{"ok":true}`. 기존 TTL 제거.
- 무기한 조회: 200 `{"key":"user:101","value":"Alex","ttl_ms":-1}`.
- 빈 값 저장: PUT `/keys/empty`, 본문 `{"value":""}` → 200. 조회는 value가 빈 문자열인 200이며 부재와 다르다.
- 삭제: 유효 키 첫 삭제는 deleted=1, 재삭제·처음부터 없음·이미 만료는 deleted=0.
- 없음·만료 조회: 404 `{"error":{"code":"KEY_NOT_FOUND","message":"키가 없거나 만료되었습니다."}}`.

### 특수문자 키

유효 UTF-8의 비어 있지 않은 키를 지원한다. 공백·한글·콜론·슬래시·물음표·샵·퍼센트를 허용하며 제어문자(NUL·개행 등)와 **키 전체가 `.` 또는 `..`인 경우**는 400으로 거부한다. `a.b`와 `a..b`는 허용한다. 키를 공백 제거·대소문자 변환·Unicode 정규화하지 않는다. 경로의 한 구간으로 정확히 한 번 인코딩해 전송하고, Go 라우터가 해석한 키에 추가 디코딩을 하지 않는다.

| 실제 키 | 요청 경로 예시 |
|---|---|
| `user:101` | `/keys/user%3A101` |
| `user name` | `/keys/user%20name` |
| `a/b` | `/keys/a%2Fb` |
| `a?b#c` | `/keys/a%3Fb%23c` |
| `a+b` | `/keys/a%2Bb` |
| 문자 그대로의 `%2F` | `/keys/%252F` |
| `민지` | `/keys/%EB%AF%BC%EC%A7%80` |

인코딩하지 않은 `/keys/a/b`는 두 경로 구간이므로 정의되지 않은 경로로 404다. `+`를 공백으로 바꾸는 쿼리 디코딩 규칙을 키 경로에 적용하지 않는다. `/keys`와 `/keys/`는 빈 키 요청으로 400이다. 키 길이는 디코딩한 UTF-8 바이트로 측정한다.

`/keys/.`·`/keys/..` 및 `%2E`·`%2E%2E`처럼 인코딩한 형태도 JSON 400이어야 한다. ServeMux의 경로 정리가 이를 리다이렉트하기 전에 요청 앞단에서 원래의 인코딩된 경로를 확인한다. 이 검증과 핸들러 모두 원문을 한 번만 해석하며 이미 해석한 키를 다시 디코딩하지 않는다. 인코딩된 슬래시를 포함한 키를 파일 경로처럼 정규화하지 않는다.

### 원본 API·소비 앱

원본은 상품 데이터를 제공하고 GET 호출 횟수를 계측한다. 소비 앱은 원본과 mini-Redis 사이의 캐시 흐름만 담당한다. 별도 소비 앱 상품 변경 API는 만들지 않는다. 무효화 검증은 **원본 PUT → mini-Redis DELETE → 소비 앱 GET** 순서로 실행한다.

| 대상·메서드·경로 | 요청 | 응답·계약 |
|---|---|---|
| 원본 GET `/products/{id}` | 경로의 id | 200 `{"id":"1001","name":"Keyboard","price":59000,"version":1}`. 없는 상품은 404 PRODUCT_NOT_FOUND. 설정한 지연 적용 |
| 원본 PUT `/products/{id}` | `{"name":"Keyboard","price":49000,"version":2}` | 생성·변경 후 200과 같은 형식의 변경 상품. 필수 필드·타입 오류는 400 INVALID_ARGUMENT |
| 원본 GET `/stats` | 없음 | 200 `{"calls":3}`. 상품 GET의 성공·404·실패 호출만 집계, PUT·/stats 자체는 제외 |
| 소비 앱 GET `/products/{id}?cache=on` | on/off. 생략은 on | 성공 시 200과 원본 상품 JSON. 다른 옵션은 원본·캐시 호출 전 400 INVALID_ARGUMENT |

원본의 price는 0 이상 정수, version은 1 이상 정수, name은 문자열이며 id는 경로값이다. 데모 id는 영문·숫자·밑줄·하이픈으로 제한한다. PUT 전에는 같은 상품 GET의 내용이 변하지 않는다. 원본 카운터는 프로세스 시작 시 0이며 테스트·측정은 /stats의 전후 차이로 판정한다. 다른 실험이 동시에 같은 원본을 사용하지 않게 한다.

소비 앱은 성공 조회에 **`X-Cache: HIT` 또는 `X-Cache: MISS`**를 반환한다. on에서 유효 캐시를 반환하면 HIT, 미적중 후 원본을 조회하면 MISS다. off는 캐시를 조회·저장하지 않고 원본만 호출하며 헤더는 MISS다. off의 MISS는 캐시 조회가 일어났다는 뜻이 아니며 mode로 구분한다. BYPASS 값은 사용하지 않는다.

on의 흐름은 캐시 GET → 404면 원본 GET → 성공 상품을 JSON 문자열로 SET → 반환이다. 캐싱 TTL은 `cache_ttl_ms` 설정으로 제공한다. 데모 기본값은 60,000ms이며 기능 테스트에서는 짧게 바꿀 수 있다. 원본 404는 그대로 404로 반환하고 부재 응답은 캐싱하지 않는다. 원본 5xx·시간 초과 또는 캐시 조회·저장 실패는 소비 앱의 502 UPSTREAM_ERROR로 반환하고 실패 응답은 캐싱하지 않는다. 원본 우회·자동 재시도·요청 합치기는 제공하지 않는다.

원본·소비 앱의 오류도 error.code·message JSON 형식이다. **외부 기능 테스트는 같은 테스트 안에서 원본·mini-Redis·소비 앱 핸들러를 각각 httptest.NewServer로 띄우고 실제 HTTP로 연결한다.** 테스트 서버가 반환한 URL을 소비 앱·테스트 클라이언트에 전달하고 종료는 테스트 정리 작업에 등록한다. 테스트마다 새 Store·원본 데이터를 만들어 상태를 분리한다. 별도 프로세스 실행·고정 포트·준비 대기는 사용하지 않는다. mini-Redis에 추가 명령·관리 API를 도입하지 않는다.

## 5. 입력 검증·오류 코드

이 표를 **2단계 구현 전에 확인하는 상태 코드 기준**으로 사용한다. 모든 오류는 아래 JSON 형식이며 message 문구는 판정에 사용하지 않는다.

오류 예시: `{"error":{"code":"INVALID_ARGUMENT","message":"ttl_ms는 양의 정수여야 합니다."}}`.

| 조건 | HTTP | code | 기존 데이터 |
|---|---:|---|---|
| 빈 키, 유효하지 않은 UTF-8·제어문자 키, 키 전체가 `.` 또는 `..` | 400 | INVALID_ARGUMENT | 유지 |
| 디코딩한 키가 256바이트 초과 | 413 | LIMIT_EXCEEDED | 유지 |
| 디코딩한 값이 65,536바이트 초과 | 413 | LIMIT_EXCEEDED | 유지 |
| PUT 본문이 설정한 최대 바이트 초과 | 413 | LIMIT_EXCEEDED | 유지 |
| 잘못된 JSON, value 누락·null·문자열 외 타입 | 400 | INVALID_ARGUMENT | 유지 |
| ttl_ms가 0·음수·비정수·문자열·null 또는 안전한 표현 범위 초과 | 400 | INVALID_ARGUMENT | 유지 |
| 알 수 없는 본문 필드 | 400 | INVALID_ARGUMENT | 유지 |
| GET의 없는 키·만료된 키 | 404 | KEY_NOT_FOUND | 만료를 발견하면 물리 정리 가능 |
| 정의되지 않은 경로 | 404 | ROUTE_NOT_FOUND | 유지 |
| 알려진 키 경로의 미지원 메서드 | 405 | METHOD_NOT_ALLOWED | 유지·Allow는 정확히 GET, PUT, DELETE |
| PUT에서 다른 미디어 타입을 명시하거나 잘못된 Content-Type을 명시 | 415 | UNSUPPORTED_MEDIA_TYPE | 유지 |
| 처리 가능한 예기치 않은 내부 실패 | 500 | INTERNAL_ERROR | 내부 오류 로그 기록, 성공으로 응답하지 않음 |
| AOF 기록·Sync 실패, 실행 전 대기 취소·파일 상한, 종료 후 변경 | 503 | PERSISTENCE_UNAVAILABLE | 현재 요청의 메모리 반영을 확인하지 못했거나 변경을 수락하지 않음. 앞선 미동기화 변경 유실 가능 |
| 실행 중 기한 초과·취소, 변경 작업 panic | 503 | PERSISTENCE_OUTCOME_UNKNOWN | 반영 여부 불명. 나중에 반영될 수 있으므로 조회로 확인하고 무조건 재시도하지 않음 |

정확히 256바이트 키·65,536바이트 값은 허용한다. **Content-Type 헤더가 없으면 JSON으로 간주**하고, application/json의 charset=utf-8 매개변수도 허용한다. text/plain 등 다른 타입을 명시했을 때만 415이며, JSON으로 간주한 본문이 잘못됐으면 400이다. 이 정책은 JSON 본문을 받는 원본 PUT에도 적용한다. 요청 크기·Content-Type·JSON 구문·필드 타입을 확인한 뒤 저장 규칙 검증으로 진행한다. JSON 숫자는 소수 부분이 없는 정수 값인지 확인한다. 하나의 본문에 JSON 값이 여러 개 이어지면 400이다.

값·TTL 검증 실패로 기존 데이터를 지우거나 TTL을 바꾸지 않는다. HTTP 요청줄 자체가 깨져 Go 서버가 핸들러 진입 전에 거부하는 경우에는 상태 코드만 보장하며 애플리케이션 오류 JSON 계약에서 제외한다. 메모리 전체 상한은 없으며 프로세스 수준의 메모리 고갈을 안전하게 500으로 복구한다고 약속하지 않는다.

## 6. 실행 설정·디버그 로그

| 설정 | 기본값·계약 |
|---|---|
| Go 버전 | 로컬 Homebrew 설치 기준, **Go 1.22 이상**. go.mod 언어 기준은 1.22.0, 특정 패치 버전·toolchain 지시문 고정 없음. |
| 실행 환경 | Mac에 설치된 Go로 직접 실행·테스트. 실제 사용 버전은 실행·성능 결과에 기록. 외부 모듈 의존 없음 |
| 서버 주소 | `MINI_REDIS_ADDR=127.0.0.1:8080` |
| 디버그 | `MINI_REDIS_DEBUG=false`. `-debug` 플래그로 덮어쓰기 가능. 우선순위: 명시적 플래그 > 환경변수 > 기본값 |
| 본문 최대 크기 | `MINI_REDIS_MAX_BODY_BYTES=524288`(512 KiB). 실제 수신 본문 기준으로 제한 |
| ReadHeaderTimeout | 3초. 요청 헤더 읽기 |
| ReadTimeout | 5초. 요청 읽기 |
| WriteTimeout | 5초. 응답 쓰기 |
| IdleTimeout | 60초. 재사용 연결의 대기 |
| 종료 대기 | HTTP 요청 종료·AOF 최종 Sync를 합쳐 5초. 초과 시 오류 종료 |
| AOF | MINI_REDIS_AOF=true, MINI_REDIS_AOF_PATH=data/appendonly.aof. false는 파일을 읽거나 변경하지 않는 메모리 전용 |
| AOF 비용 제한 | 1초 Sync, 미동기화 3초 초과 시 변경 거부. 별도로 영속성 변경 결과 대기를 3초로 제한. MINI_REDIS_AOF_MAX_BYTES=268435456은 파일+DEL 예약량 한도. MINI_REDIS_AOF_REWRITE_MIN_BYTES=4194304. 변경분 버퍼 최대 8 MiB |
| 성능 실험 | 원본 127.0.0.1:8081, 소비 앱 127.0.0.1:8082. 지연·주소·데모 TTL은 실험 설정으로 전달 |
| 소비 앱 HTTP 클라이언트 | 공유 클라이언트·Transport 재사용. MaxIdleConnsPerHost를 동시 요청 수 이상으로 설정, 기본 20 이상 |
| bench HTTP 클라이언트 | 공유 클라이언트·Transport 재사용. MaxIdleConnsPerHost를 `-concurrency` 이상으로 설정 |
| 클라이언트 시간 제한 | 소비 앱의 원본·캐시 호출과 bench 요청은 기본 3초. 실패·시간 초과를 오류로 집계 |

키·값 한도, 초기 버킷 16·적재율 0.75·2배 확장은 구현 상수로 고정한다. 주소·본문 설정이 유효하지 않으면 시작 실패로 알린다. 네트워크 타임아웃은 저장 연산의 롤백을 뜻하지 않는다. 별도 요청 취소·재시도·중복 방지 기능은 이번 범위에 추가하지 않는다.

소비 앱과 bench에서 요청마다 HTTP 클라이언트를 새로 만들지 않는다. 응답 본문을 끝까지 읽고 닫아 연결을 재사용한다. 전체 MaxIdleConns도 접근 대상별 연결을 수용하도록 설정하고, MaxConnsPerHost에 별도 상한을 두면 동시 요청 수보다 작게 두지 않는다. 부하를 높일 때 소비 앱의 동시 요청 설정도 함께 맞춰 연결 재사용 조건이 달라지지 않게 한다.

디버그는 stderr에 기록하고 기본 비활성화한다. 활성화 시 다음을 관찰할 수 있어야 한다.

| 로그 사건 | 필수 관찰 항목 |
|---|---|
| HASH·LOOKUP | 작업명, 키, 해시값, 버킷 번호 |
| COLLISION·COMPARE | 요청 키·기존 키, 비교 결과 |
| INSERT·UPDATE·DELETE | 변경 종류, 물리 키 수 |
| RESIZE·MOVE | 이전/새 버킷 수, 이동 키와 이전/새 위치 |
| EXPIRATION | Missing/Live/Expired, 무기한 여부·남은 TTL, 정리 여부 |

전체 값·원본 응답 본문은 기본 로그에 넣지 않는다. 디버그 활성 여부는 생성 시 한 번 결정하며 실행 중 로그 수준 변경은 지원하지 않는다. 모든 해시 테이블 디버그 호출은 활성 여부를 먼저 검사해, 비활성 시 로그 인자 준비를 생략한다. 비활성 시 상세 문자열·스냅샷도 만들지 않는다. 전체 연산의 메모리 할당이 없음을 보장하는 것은 아니다. **성능 비교는 서버·소비 앱 모두 디버그를 반드시 끈다.**

## 7. 완료 기준·필수 테스트·성능 결과·캐시 키

### 필수 테스트 이름과 판정

각 행의 이름은 실제 테스트 함수 이름으로 사용한다.

| 계층 | 필수 테스트 이름 | 판정 |
|---|---|---|
| ③ | TestHashFNV1aKnownVectors | 알려진 FNV-1a 결과와 일치 |
| ③ | TestTableSetGetUpdateAndEmptyValue | 새 저장·갱신·빈 값·부재 구분, 개수 유지 |
| ③ | TestTableCollisionDeletePositions | 실제 충돌 강제, 첫·중간·끝·없는 키 삭제 후 다른 키 보존 |
| ③ | TestTableResizeThresholdAndPreservation | 12키까지 16버킷, 13번째 새 키에서 32버킷. 갱신은 확장 없음, 반복 확장 후 값·만료 정보·개수 보존 |
| ② | TestStoreTTLBoundaryAndRemaining | 만료 직전·정각·이후, 올림된 TTL·조회 비연장 |
| ② | TestStoreTTLTransitions | 무기한↔TTL, 연장·단축, 만료 후 재저장 |
| ② | TestStoreMissingExpiredAndDelete | 내부 없음/만료 구분, 만료 삭제의 Deleted=false, 재등록 보존 |
| ② | TestStoreValidationPreservesValueAndTTL | 5절 입력 오류와 상한 경계, 잘못된 요청이 기존 상태를 변경하지 않음 |
| ② | TestStoreConcurrentOperations | 같은 키·다른 키의 동시 연산, 값·TTL 조합·최종 상태·교착 없음. 레이스 탐지 병행 |
| ① | TestHTTPKeyRoundTrip | 특수문자 키·UTF-8 값·빈 값 왕복, 이중 디코딩 없음. 점 키의 원문·인코딩 형태는 400 |
| ① | TestHTTPValidationAndErrors | 404·405도 JSON, Allow는 GET, PUT, DELETE. Content-Type 생략 수용·명시한 다른 타입 거부, 5절 상태·code·본문 한도 검증 |
| ① | TestHTTPGetRemainingTTL | 값·ttl_ms 함께 반환, 무기한 -1, 없음/만료 동일 404 |
| 외부 | TestCacheMissHitAndKeyIsolation | 첫 MISS·원본 1회, 다음 HIT·추가 0회, 서로 다른 상품 독립 |
| 외부 | TestCacheExpiryAndInvalidation | TTL 후 재조회. 원본 PUT→mini-Redis DEL→소비 앱 GET의 새 값 반영 |

### 선택 테스트

다음 네 개는 필수 완료 조건에서 제외한다. 관련 기능 계약 자체를 제거하는 것은 아니다. 예를 들어 디버그 스위치는 수동 확인하고 성능 측정에서는 항상 끈다.

| 계층 | 선택 테스트 이름 | 목적 |
|---|---|---|
| ③ | TestTableRandomizedReferenceModel | 고정 시드 연산을 참조 모델과 대조 |
| 설정 | TestDebugLoggingSwitch | debug on/off를 자동 검증 |
| 외부 | TestCacheBypassPreservesExistingCache | off의 기존 캐시·TTL 보존을 별도 자동 검증 |
| ② | TestStoreConcurrentExpirationAndResize | 만료·갱신·삭제·재등록·확장의 복합 경합을 추가 검증 |

가짜 시계로 시간 경계를 제어하고 실제 sleep의 1ms 차이로 판정하지 않는다. 코어 전체 키 개수는 내부 테스트로, 외부 요청 결과는 같은 테스트의 httptest.NewServer 세 개를 연결해 확인한다. 각 구현 단계 끝에는 실행·예상 출력, 직접 입력할 명령 3~5개와 설명, 디버그 로그, 격리된 실패 실험 1개, 해당 단계 테스트와 실행 명령을 함께 제공한다. 사용자가 “다음”이라고 하기 전 다음 단계로 진행하지 않는다.

### 성능 비교 기준과 출력 형식

| 조건 | 확정값 |
|---|---|
| 원본 | 고정 상품 JSON 응답, 인위적 지연 100ms, 호출 수 계측 |
| 데이터·부하 | 상품 100개, 요청자 20명, 같은 데이터·키 분포·연결 정책 |
| 길이 | **준비 3초·측정 10초·1회**. 플래그로 변경 가능. 반복을 늘리면 사용/미사용 교대 |
| 사용 조건 | 상품 100개(또는 -products 설정 개수)를 소비 앱 cache=on으로 각각 1회 조회해 사전 저장. 전체 실험 종료까지 만료되지 않는 TTL 설정 |
| 측정 | 소비 앱 요청 시작부터 본문 전체 수신, 성공 완료 수/초, 오류율, 적중률, 원본 GET 증분 |

`cmd/bench`가 준비·측정·통계·CSV 출력을 담당한다. 플래그는 `-warmup`(기본 3s), `-duration`(10s), `-runs`(1), `-concurrency`(20), `-products`(100), `-demo-url`, `-origin-url`로 고정한다. demo·origin URL의 기본값은 6절 주소다. warmup은 0 이상, duration·runs·concurrency·products는 양수여야 하며 잘못된 설정은 측정 전 거부한다. 소비 앱의 연결 재사용 설정도 동일한 동시 요청 수에 맞춘다.

bench는 빈 캐시·준비된 상품 데이터로 시작하고, **측정 전에 상품 100개를 cache=on으로 각각 1회 조회해 웜 캐시를 채운다.** -products를 변경하면 그 개수만큼 동일하게 준비한다. 소비 앱의 `cache_ttl_ms`는 사전 저장 시작부터 모든 run의 on/off 준비·측정 종료까지 걸리는 전체 시간보다 길게 설정하며 여유를 둔다. 기본 60,000ms가 변경한 실험 길이에 부족하면 소비 앱 시작 설정에서 늘린다.

사전 저장·준비·측정을 구분하고 사전 저장·준비 구간은 CSV 집계에서 제외한다. 원본 호출 수는 `/stats`의 측정 전후 차이로 확인하며 계측 요청은 측정 요청 수에 포함하지 않는다. stdout에는 CSV 헤더·행만, 진행 상태·환경 정보·오류는 stderr에 출력한다.

출력은 다음 CSV 열 순서로 고정한다.

`run,mode,concurrency,requests,successes,errors,p50_ms,p95_ms,success_rps,hit_rate,origin_calls,debug`

mode는 on/off, 비율은 0~1, debug는 false다. 한 run에 on·off 행을 각각 출력한다. hit_rate는 측정 성공 응답 중 HIT 비율이며 off는 0으로 출력한다. off의 MISS 헤더를 실제 캐시 조회 횟수로 해석하지 않는다. on의 정상 웜 실험은 원본 추가 호출 0, off는 요청별 원본 호출을 확인한다. 실패 응답을 적중으로 집계하지 않는다. 실행 환경·Go 버전·커밋·원본 지연·TTL·부하 설정은 별도 메타데이터로 기록한다. 실행별 p95를 전체 요청 p95처럼 합치지 않으며 고정 개선율·처리량 하한은 두지 않는다.

### 캐시 키 이름 규칙

- 데모 상품 키: **`product:{id}`**. 예: 상품 ID 1001 → `product:1001`.
- id는 데모에서 영문·숫자·밑줄·하이픈만 사용한다. 응답이 다른 상품은 반드시 다른 키다.
- 캐시 키 원문과 HTTP 경로 인코딩을 구분한다. `product:1001`을 경로에 넣을 때만 인코딩하고 저장 키에 인코딩 문자열을 중복 저장하지 않는다.
- 단위·외부 테스트는 케이스별 `test:{case}:{key}`를 쓰거나 Store 인스턴스·서버를 분리한다. 벤치마크는 준비한 상품 키 100개를 사용한다.
- **테스트/사용자가 원본 PUT 후 mini-Redis에 같은 상품 캐시 키를 DEL한다.** 소비 앱은 상품 조회만 제공한다. 패턴 삭제·태그·자동 축출은 구현하지 않는다.

설계 참고: [Go HTTP 경로 구간 처리](https://pkg.go.dev/net/http#ServeMux), [Go 단조 시계](https://pkg.go.dev/time#hdr-Monotonic_Clocks).
