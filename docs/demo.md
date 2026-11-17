# 캐싱 데모: 원본 API와 소비 앱

`cmd/demo`는 캐싱을 눈으로 확인하기 위한 두 서비스를 한 번에 띄웁니다. mini-Redis는 따로 실행합니다.

```text
클라이언트 ──> 소비 앱 (8082) ──> mini-Redis (8080)   캐시 조회·저장·삭제
                    └──────────> 원본 API (8081)      미적중·cache=off일 때 조회
```

- **원본 API**: 진짜 데이터를 가진 느린 서버입니다. 기본 지연 100ms, 상품 1~100번을 갖고 시작합니다.
- **소비 앱**: 상품 요청을 받아 mini-Redis를 캐시로 사용합니다(Cache-Aside). mini-Redis는 HTTP로만 호출하며 내부 패키지를 직접 쓰지 않습니다.

경로와 동작은 [팀 PRD](learning/PRD.md) 2장의 데모 인터페이스를 따릅니다.

## 실행

터미널 두 개를 사용합니다.

```bash
# 터미널 1: mini-Redis (데모는 메모리 모드 권장)
MINI_REDIS_AOF=false go run ./cmd/miniredis

# 터미널 2: 원본 API + 소비 앱
go run ./cmd/demo
# origin  http://127.0.0.1:8081  (delay 100ms, products 1..100)
# app     http://127.0.0.1:8082  (cache http://127.0.0.1:8080, ttl 60000ms)
```

세 서비스 모두 `GET /health`가 200을 반환하면 준비된 것입니다. 공용 스크립트에는 기준 URL로 `http://127.0.0.1:8081`(원본), `http://127.0.0.1:8082`(소비 앱), `http://127.0.0.1:8080`(mini-Redis)을 전달합니다.

| 옵션 | 기본값 | 의미 |
|---|---|---|
| `-origin-addr` | `127.0.0.1:8081` | 원본 API 주소 |
| `-app-addr` | `127.0.0.1:8082` | 소비 앱 주소 |
| `-redis-url` | `http://127.0.0.1:8080` | mini-Redis 기준 URL |
| `-origin-url` | 없음 | 지정하면 원본을 새로 띄우지 않고 이미 실행 중인 원본을 사용 |
| `-origin-delay` | `100ms` | 원본 상품 조회의 인위적 지연 |
| `-cache-ttl-ms` | `60000` | 캐시에 저장할 때의 TTL. 기능 테스트는 `1000` 권장 |
| `-products` | `100` | 시작 시 넣어 둘 상품 수(id 1..N) |
| `-timeout` | `3s` | HTTP 클라이언트 제한 시간. 원본 호출에 적용되며 캐시 호출도 이 값을 넘지 않음 |
| `-cache-timeout` | `200ms` | 캐시 조회·저장·삭제 각각의 제한 시간. `-timeout`과 요청 취소도 함께 적용 |
| `-cache-fallback` | `false` | 조회 시 캐시 오류가 나면 원본으로 우회하는 P1 확장 |

mini-Redis에 인증 토큰을 설정했다면 같은 값을 환경변수 `MINI_REDIS_AUTH_TOKEN`으로 `cmd/demo`에도 전달합니다.

## 직접 확인하기

```bash
# 첫 요청: 캐시에 없어서 원본까지 다녀옵니다 (약 100ms)
curl -si 'http://127.0.0.1:8082/demo/products/42' | grep -iE 'x-cache|^\{'
# X-Cache: MISS
# {"id":"42","name":"product-42","version":1}

# 두 번째 요청: 캐시에서 바로 응답합니다 (약 1ms)
curl -si 'http://127.0.0.1:8082/demo/products/42' | grep -i x-cache
# X-Cache: HIT

# 캐시를 쓰지 않으면 매번 원본을 호출합니다
curl -si 'http://127.0.0.1:8082/demo/products/42?cache=off' | grep -i x-cache
# X-Cache: BYPASS

# 원본이 실제로 몇 번 호출됐는지 확인
curl -s 'http://127.0.0.1:8081/test/counters'
# {"get_calls":2}

# 소비 앱으로 상품을 바꾸면 원본 변경 후 캐시 키를 삭제합니다(무효화)
curl -s -X PUT 'http://127.0.0.1:8082/demo/products/42' \
  -H 'Content-Type: application/json' -d '{"name":"renamed","version":2}'
curl -si 'http://127.0.0.1:8082/demo/products/42' | grep -iE 'x-cache|^\{'
# X-Cache: MISS
# {"id":"42","name":"renamed","version":2}
```

응답 시간을 함께 보려면 `curl -s -o /dev/null -w '%{time_total}\n' <URL>`을 사용합니다.

## 인터페이스

### 원본 API

| 요청 | 동작 |
|---|---|
| `GET /products/{id}` | 200 상품 JSON. 없는 상품은 404. 실패 모드에서는 500. 설정한 지연 적용 |
| `PUT /products/{id}` | `{"name":"sample","version":2}`로 생성·변경. 200과 변경된 상품. id는 경로 값 사용 |
| `GET /test/counters` | 200 `{"get_calls":3}`. 상품 GET을 성공·실패 모두 집계 |
| `DELETE /test/counters` | 200 `{"ok":true}`. 카운터 초기화 |
| `PUT /test/fail` | `{"enabled":true}`면 상품 GET이 500, `false`면 복구. 200 `{"ok":true}` |

### 소비 앱

| 요청 | 동작 |
|---|---|
| `GET /demo/products/{id}?cache=on` | 캐시 `product:{id}` 우선. 생략하면 on. 성공 시 200과 원본 상품 JSON |
| `GET /demo/products/{id}?cache=off` | 항상 원본 조회. 캐시를 읽지도 쓰지도 않음 |
| `PUT /demo/products/{id}` | 원본 변경 후 `product:{id}` 삭제. 둘 다 성공하면 200과 변경된 상품 |

기본 모드에서 성공한 조회의 `X-Cache` 헤더는 `HIT`(캐시 반환), `MISS`(미적중 후 원본 조회·저장), `BYPASS`(cache=off)입니다.

| 상황 | 응답 |
|---|---|
| `cache` 값이 on/off가 아님 | 400. 원본·캐시에 접근하지 않음 |
| 원본에 없는 상품 | 404. 부재는 캐싱하지 않음 |
| 원본 5xx·시간 초과 | 502. 실패 응답은 캐싱하지 않음 |
| 캐시 조회·저장·삭제 실패 | 502. 캐시 GET의 404는 실패가 아니라 미적중 |
| 원본 변경 실패 | 캐시 삭제로 진행하지 않음 |

## 선택형 캐시 장애 우회

```bash
go run ./cmd/demo -cache-fallback -cache-timeout 200ms
```

기본값은 꺼짐으로 공통 PRD의 502 계약을 유지합니다. 켜면 상품 GET에만 다음 규칙을 적용합니다.

| 상황 | 동작·표시 |
|---|---|
| 캐시 GET 실패(시간 초과·연결 실패·401·잘못된 응답 등) | 원본 조회 성공 시 200, `X-Cache: BYPASS-ERROR`. 캐시 PUT은 건너뜀 |
| 캐시 미적중 후 원본 200, 캐시 PUT 실패(용량 초과 포함) | 원본 응답을 200으로 반환, `X-Cache: MISS`. 저장 성공을 보장하는 헤더는 아님 |
| 정상 캐시 HIT·MISS 또는 cache=off | 기존 동작 유지 |
| 원본 404 또는 실패 | 기존 404·502 유지. 실패 응답은 캐싱하지 않음 |
| 상품 PUT 후 캐시 DELETE 실패 | 우회 설정과 무관하게 502. 원본은 이미 변경됐을 수 있음 |

캐시 실패는 기본 로그에 `CACHE_FAILURE`와 메서드·상태 코드·실패 분류를 남깁니다. 401도 기록하므로 토큰 설정을 확인하세요. 키·값·토큰·URL·응답 본문은 기록하지 않습니다. 로그의 `fallback_enabled`는 설정값이며 실제 우회 성공을 뜻하지 않습니다.

캐시 전용 제한 시간은 호출마다 적용됩니다. 정상 미적중은 GET과 PUT 각각 최대 한 번의 캐시 대기가 있을 수 있습니다. 캐시 제한 시간이 끝나도 원본 호출은 원래 요청의 컨텍스트와 `-timeout`을 사용합니다. 클라이언트 요청 취소는 그대로 전파됩니다. 원본 우회는 요청 합치기나 부하 제한을 제공하지 않으므로 장애 시 원본 부하가 증가할 수 있습니다. 공용 성능 비교는 기본 우회 비활성 상태로 수행합니다.

## 한계

- 캐시와 원본을 묶는 트랜잭션은 없습니다. 원본 변경 후 캐시 삭제가 실패하면 원본은 이미 바뀐 상태로 502를 반환합니다.
- 원본 조회와 캐시 저장 사이에 다른 요청이 무효화하면 예전 값이 다시 저장될 수 있습니다. [캐시 연동 방법과 경합 한계](cache-aside.md)를 참고하세요.
- 같은 키의 동시 미적중을 하나로 합치지 않으므로 원본 호출이 겹칠 수 있습니다.
- 캐시 장애 시 원본 우회는 `-cache-fallback`으로 명시적으로 켜는 PRD P1 확장입니다.
- 시연·기능 검증용입니다. 성능 수치는 조건을 통제하는 [측정 도구](benchmark.md)로 냅니다.

## 테스트

`cmd/demo/main_test.go`가 원본·mini-Redis·소비 앱을 각각 HTTP 서버로 띄워 PRD의 F2~F8을 검증합니다. TTL은 가짜 시계를 전진시켜 확인하므로 sleep에 의존하지 않습니다.

`cmd/demo/fallback_test.go`는 우회 on/off, 실제 용량 거부, 인증 오류 로그, 캐시 시간 초과와 원본 제한 분리, 우회 중 무효화 실패 502를 검증합니다.

```bash
go test ./cmd/demo -count=1 -v
```
