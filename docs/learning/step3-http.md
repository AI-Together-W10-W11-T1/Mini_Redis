# 3단계: HTTP로 저장·조회·삭제하기

## 1. Store 검수 결과

기능을 막는 오류는 발견하지 않았다. 키 길이를 UTF-8 순회보다 먼저 검사하는 변경과 관련 경계 테스트는 타당하다. 만료 정리·덮어쓰기·확장을 함께 실행하는 추가 테스트도 통과했다. 이 테스트는 새 grow 키의 값까지 확인하도록 보완했다.

측정 설명은 수정했다. `BenchmarkStoreParallel`의 `ns/op`는 전체 실행 시간/연산 수이며 락 보유 시간이나 개별 요청 지연이 아니다. 따라서 이 수치만으로 HTTP에서 락이 병목이 아니라고 단정하지 않는다. 벤치마크 준비 작업은 `ResetTimer`로 측정에서 제외했다. 비교 구현·원시 결과가 없는 과거 성능 수치를 현재 설계의 검증 근거로 사용하지 않는다.

기존 단일 Mutex·지연 만료 정책은 유지한다. 주기적 만료 정리·샤딩·점진적 리사이징은 도입하지 않는다.

## 2. 어디에 무엇을 구현했나

```text
curl → ① HTTP: 경로·메서드·본문 검사
              ↓
       ② Store: 입력 규칙·락·TTL
              ↓
       ③ HashTable: 저장·탐색·삭제·확장
```

| 파일 | 역할 |
|---|---|
| internal/httpapi/handler.go | HTTP 경로·메서드, JSON 해석, Store 호출, JSON 응답 |
| internal/httpapi/handler_test.go | 특수문자·입력 오류·TTL·내부 오류·실패 실험 |
| internal/config/config.go | 환경변수·debug 플래그 검증, 주소·본문 상한·타임아웃 |
| internal/config/config_test.go | 기본값·설정 우선순위·잘못된 설정 검사 |
| cmd/miniredis/main.go | 설정 → 로거 → Store → HTTP 서버 조립, 종료 처리 |

### 함수별 핵심

| 함수 | 역할 → 로직 → 기억할 점 |
|---|---|
| NewHandler | 기본 512 KiB 본문 상한으로 핸들러 생성 |
| NewHandlerWithMaxBodyBytes | 같은 동작에 설정한 양의 본문 상한 적용. 설정용 생성자 |
| ServeHTTP | 원래 경로의 점 키·구간 수 확인 → 라우터 실행. 정리 리다이렉트 전에 거부 |
| ServeKey | GET·PUT·DELETE로 분기. 나머지는 JSON 405와 Allow |
| Set | 실제 본문 크기 → Content-Type → JSON 필드·TTL → Store.Set → ok |
| parseTTL | 숫자 텍스트를 정확하게 해석. float64 반올림 없이 정수 값·범위 확인 |
| Get | Store.Get → 유효하면 값·남은 TTL, 없음·만료는 같은 JSON 404 |
| Delete | Store.Delete의 Deleted를 숫자 1 또는 0으로 변환 |
| writeJSON / writeError | 성공·오류 모두 application/json 응답. 상세 값은 로그에 넣지 않음 |
| config.Load | 기본값 → 환경변수 → 명시적 debug 플래그. 잘못된 설정은 시작 실패 |
| run | 서버 시작 → 종료 신호 대기 → 최대 5초 동안 진행 요청 종료 대기 |

API는 PUT·GET·DELETE `/keys/{key}`다. `/keys`·`/keys/`는 400, 알 수 없는 경로는 404다. `/keys/{key}`는 메서드 없는 패턴 하나에 등록한다. 키가 `/` 하나인 인코딩 경로는 라우터의 끝 슬래시 패턴 처리와 충돌할 수 있어 실제 원래 경로를 구분한다. 점 키는 400이며 `/keys/a/b`처럼 인코딩하지 않은 추가 구간은 404다.

키는 원래 경로 구간에서 한 번 해석한 값 또는 라우터 PathValue를 사용한다. PathValue를 다시 디코딩하지 않는다. `/keys/%2F`는 키 `/`, `/keys/%252F`는 키 `%2F`이며 서로 독립이다.

TTL 생략은 무기한, null·문자열·비정수·0·음수·범위 초과는 400이다. JSON 숫자 `1.0`이나 `1e1`은 각각 정수 값 1·10으로 수용한다. 빈 값과 빈 키를 구분한다. 오류 코드·크기 경계는 design.md 5절과 같다.

## 3. 서버 실행과 예상 출력

프로젝트 루트의 터미널 A:

```bash
MINI_REDIS_DEBUG=true go run ./cmd/miniredis
```

예상 시작 로그(시각은 실행마다 다름):

```text
level=INFO msg=START addr=127.0.0.1:8080 debug=true max_body_bytes=524288
```

기본 주소는 `127.0.0.1:8080`이며 `MINI_REDIS_ADDR`로 변경한다. `MINI_REDIS_MAX_BODY_BYTES`는 기본 524288이다. `-debug=false`를 명시하면 환경변수보다 우선한다. 성능 측정은 반드시 debug=false로 실행한다.

서버는 Ctrl+C로 종료한다. SIGTERM도 같은 종료 절차를 사용하며 STOP 로그를 남긴다. 현재 AOF 기본 활성화로 정상 종료 시 최종 Sync를 수행한다. 메모리 전용 모드에서는 데이터를 보존하지 않는다. 최신 보장 범위는 docs/step4-persistence.md를 따른다. 헤더 읽기 3초, 읽기·쓰기 5초, 유휴 연결 60초, 전체 종료 대기 5초를 적용한다.

## 4. 터미널 B에서 직접 해볼 명령 5개

### ① 3초 동안 저장

```bash
curl -sS -X PUT 'http://127.0.0.1:8080/keys/user%3A101' -H 'Content-Type: application/json' -d '{"value":"Minji","ttl_ms":3000}'
```

예상: `{"ok":true}`. 현재 시각부터 3초 뒤 만료되는 값이 저장된다.

### ② 바로 조회

```bash
curl -sS 'http://127.0.0.1:8080/keys/user%3A101'
```

3초 이내라면 `{"key":"user:101","value":"Minji","ttl_ms":...}`. 남은 TTL은 조회 시점에 따라 달라진다. 입력이 늦어 3초가 지났으면 404가 정상이다.

### ③ 만료 후 조회

```bash
sleep 3
curl -sS -i 'http://127.0.0.1:8080/keys/user%3A101'
```

예상: HTTP 404와 `{"error":{"code":"KEY_NOT_FOUND","message":"키가 없거나 만료되었습니다."}}`. 요청 시 만료 항목을 정리한다.

### ④ TTL 없이 다시 저장

```bash
curl -sS -X PUT 'http://127.0.0.1:8080/keys/user%3A101' -H 'Content-Type: application/json' -d '{"value":"Jisoo"}'
```

예상: `{"ok":true}`. 이번 값은 무기한이며 조회 시 ttl_ms=-1이다.

### ⑤ 명시적으로 삭제

```bash
curl -sS -X DELETE 'http://127.0.0.1:8080/keys/user%3A101'
```

예상: `{"deleted":1}`. 같은 명령을 반복하면 `{"deleted":0}`이다.

Content-Type 없이 직접 보낸 JSON도 허용한다. curl의 `-d`는 기본적으로 다른 Content-Type을 붙이므로 위 명령에서는 application/json을 명시한다. 헤더 생략을 실험하려면 `-H 'Content-Type:'`로 curl의 자동 헤더를 제거한다. 다른 타입을 명시하면 415다.

## 5. 디버그 로그

요청 → 해시 계산 → Store 판단 → 응답을 같은 로그에서 추적한다.

```text
level=DEBUG msg=HTTP_REQUEST method=GET path=/keys/ttl
level=DEBUG msg=HASH op=get key=ttl hash=... bucket=...
level=DEBUG msg=EXPIRATION op=get key=ttl state=Live unlimited=false ttl_ms=1 cleaned=false
level=DEBUG msg=HTTP_RESPONSE status=200
```

가짜 시계 테스트에서 실제 확인한 만료 경계:

```text
level=DEBUG msg=EXPIRATION op=get key=ttl state=Live unlimited=false ttl_ms=2 cleaned=false
level=DEBUG msg=EXPIRATION op=get key=ttl state=Live unlimited=false ttl_ms=1 cleaned=false
level=DEBUG msg=EXPIRATION op=get key=ttl state=Expired unlimited=false ttl_ms=-2 cleaned=true
```

내부의 -2와 Expired는 HTTP JSON에 노출하지 않는다. 모든 오류는 error.code·message 형식이며 HEAD의 경우 HTTP 규칙에 따라 실제 전송 본문은 없고 405와 JSON Content-Type·Allow 헤더가 전달된다.

## 6. 일부러 깨뜨려보기: 키를 두 번 디코딩하면?

서버를 띄울 필요 없이 프로젝트 루트에서 실행한다.

```bash
MINI_REDIS_DEBUG=true MINI_REDIS_EXPERIMENT=double-decode go test ./internal/httpapi -run '^TestExperimentWithDoubleDecoding$' -count=1 -v
```

`%2F`와 `/`를 서로 다른 값으로 저장한 뒤, 테스트 안에서만 PathValue를 다시 디코딩하는 오류를 흉내 낸다. 정상 구현은 `%2F`의 값을 반환하지만 잘못된 구현은 `/`의 값을 반환한다.

```text
의도한 실패: 키를 두 번 디코딩 → %2F 조회가 다른 키 /의 값 반환: {"key":"/","value":"slash","ttl_ms":-1}
FAIL
```

이 명령만 종료 코드 1이 기대 결과다. 제품 코드는 변경하지 않으며 평소에는 SKIP된다. 복구 작업 없이 다음 일반 테스트를 실행할 수 있다.

## 7. 테스트와 검증

| 테스트 | 검증 |
|---|---|
| TestHTTPKeyRoundTrip | 실제 httptest.NewServer로 한글·공백·슬래시·%2F 왕복, 키 독립성, 빈 값·삭제, 점 키 400 |
| TestHTTPValidationAndErrors | 400·413·415, JSON 404·405, Allow, 크기·숫자 경계, 본문 실제 수신량, 오류 시 값·TTL 보존 |
| TestHTTPGetRemainingTTL | 0.4ms 올림, 만료 정각, 없음과 같은 404, 무기한 -1, 최대 TTL·정확한 숫자 해석 |
| TestHTTPInternalError | 복구 가능한 내부 실패를 JSON 500으로 변환 |
| config/TestLoad | 환경변수·플래그 우선순위, 기본값, 잘못된 주소·본문 크기·옵션 거부 |

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go build ./...
MINI_REDIS_DEBUG=true go test ./internal/httpapi -count=1 -v
MINI_REDIS_DEBUG=false go test ./internal/httpapi -count=1 -v
```

2026-11-14 최종 결과: 위 명령 모두 종료 코드 0. 레이스 탐지 보고 없음. DEBUG on에서 요청·해시·만료·응답 로그를 확인했고 off는 DEBUG 로그 0줄이다. 의도적 이중 디코딩 실험만 예상대로 종료 코드 1이다.

외부 원본 API·소비 앱은 다음 단계에서 구현한다. 이번 HTTP 테스트의 서버는 httptest.NewServer가 같은 테스트 안에서 띄우며 가짜 시계로 TTL 경계를 검증한다.

실제 서버 실행 확인은 별도 수동 검증으로 수행했다. 임시 로컬 포트에서 PUT 200 → GET 200·남은 TTL → PATCH 405·Allow → DELETE 200·1 → GET 404를 확인했다. 실제 시계로 짧은 TTL 이후 404도 확인하고 SIGTERM 종료 코드 0·START/STOP 로그를 확인한 후 서버를 종료했다.

실행 환경은 Go 1.27.2 darwin/arm64. 검증 도구의 빌드 캐시는 `/private/tmp/mini-redis-go-build`를 사용했다. 테스트에서 로컬 포트를 열기 위해 샌드박스의 로컬 네트워크 실행 권한을 사용했다.

사용자가 “다음”이라고 요청하기 전 원본 API·소비 앱 단계로 진행하지 않는다.
