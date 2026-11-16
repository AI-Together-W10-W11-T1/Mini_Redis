# HTTP API

모든 응답 본문은 JSON입니다. 클라이언트는 `message` 대신 `error.code`로 분기합니다. 인증 토큰이 설정되어 있으면 `/keys`와 `/keys/*`에 `Authorization: Bearer <token>`이 필요합니다. 인증 스킴은 대소문자를 구분하지 않으며 토큰 자체는 정확히 일치해야 합니다. `/health`는 인증 없이 호출합니다.

## 키와 라우팅

키는 1~256 UTF-8 바이트입니다. 제어 문자·잘못된 UTF-8·`.`·`..`은 400입니다. 키는 URL 경로 구간으로 인코딩하며 한 번만 디코딩합니다. `/`는 `%2F`, 문자 그대로 `%2F`는 `%252F`입니다. 원시 슬래시가 추가된 경로와 점 경로는 정리하거나 리다이렉트하지 않습니다. `/keys`·`/keys/`는 빈 키로 400, 정의되지 않은 경로는 404입니다.

## 저장·조회·삭제

| 메서드·경로 | 요청 | 성공 응답 |
|---|---|---|
| `PUT /keys/{key}` | `{"value":"a","ttl_ms":60000}` | `200 {"ok":true}` |
| `GET /keys/{key}` | 본문 없음 | `200 {"key":"x","value":"a","ttl_ms":59999}` |
| `DELETE /keys/{key}` | 본문 없음 | `200 {"deleted":1}` 또는 `200 {"deleted":0}` |

`value`는 필수 문자열이며 빈 문자열은 허용하고 최대 65,536바이트입니다. `ttl_ms`는 생략하거나 1~9,223,372,036,854의 정수 값을 지정합니다. 0·음수·null·문자열·정수가 아닌 값은 400입니다. 정수로 해석되는 JSON 숫자 표기(예: `0.5e1`)는 허용합니다. 알 수 없는 필드·복수 JSON 값은 400입니다. 중복 필드는 Go JSON 디코더에 따라 마지막 값이 적용됩니다. 잘못된 UTF-8이 JSON 문자열 안에 있으면 Go JSON 디코더가 대체 문자로 바꿀 수 있습니다.

Content-Type은 생략하거나 `application/json`(charset 지정 시 utf-8)이어야 합니다. 본문 상한 기본값은 512 KiB입니다. 덮어쓰기는 TTL도 교체하며 생략하면 기존 TTL을 제거합니다. 조회 TTL은 밀리초 올림, 무기한 -1이며 조회로 연장되지 않습니다. 없거나 만료된 키 조회는 404입니다.

지원하지 않는 키 API 메서드는 405, `Allow: GET, PUT, DELETE`입니다. 인증 실패 시 메서드·본문 검증보다 401이 우선할 수 있습니다.

## 헬스 체크

`GET /health` 정상 응답은 `200 {"status":"ok","persistence":"aof"}`이고 메모리 모드는 `disabled`입니다. 영속성 fatal 오류·작업 panic·닫힌 Store 또는 3초 넘게 밀린 미동기화/실행 중 Write가 있으면 `503 {"status":"degraded","persistence":"error"}`입니다. 상태는 atomic 값으로 읽으며 정상적인 잠금 경합은 503 사유가 아닙니다. 용량이 거의 찬 상태를 모두 감지하는 지표는 아닙니다. 파일 경로·상세 오류·키 수는 노출하지 않습니다. 다른 메서드는 JSON 405, `Allow: GET`입니다.

## 오류

```json
{"error":{"code":"INVALID_ARGUMENT","message":"설명"}}
```

| HTTP | code | 의미·대응 |
|---|---|---|
| 400 | INVALID_ARGUMENT | 키·JSON·TTL 등 입력 수정 |
| 401 | UNAUTHORIZED | 토큰 확인. `WWW-Authenticate: Bearer` |
| 404 | KEY_NOT_FOUND | 정상적인 캐시 미적중으로 처리 |
| 404 | ROUTE_NOT_FOUND | 경로 확인 |
| 405 | METHOD_NOT_ALLOWED | Allow 헤더 확인 |
| 413 | LIMIT_EXCEEDED | 키·값·본문 크기 축소 |
| 415 | UNSUPPORTED_MEDIA_TYPE | Content-Type 수정 |
| 503 | CAPACITY_EXCEEDED | 데이터 상한. 삭제·만료 회수 또는 설정 증설 |
| 503 | PERSISTENCE_UNAVAILABLE | 영속성 실패·지연·상한 또는 실행 전 거부. 원인·서버 상태 확인 |
| 503 | PERSISTENCE_OUTCOME_UNKNOWN | 이미 시작한 작업 결과 불명. 나중에 반영 가능. 무조건 재시도 금지 |
| 500 | INTERNAL_ERROR | 예기치 않은 서버 오류. 로그 확인 |

입력·데이터 용량 거부는 기존 유효 값과 TTL을 바꾸거나 해당 요청의 AOF 기록을 추가하지 않습니다. 용량 검증 과정에서 이미 만료된 항목을 회수할 수 있습니다. 영속성 오류에서 응답 코드만으로 재시작 후 상태까지 단정하지 마세요. 디스크 기록 도중의 오류는 파일에 일부 기록을 남길 수 있습니다. 결과 불명·연결 중단은 조회와 애플리케이션 정책으로 확인하며 정확히 한 번 처리는 제공하지 않습니다. 같은 값의 PUT도 TTL을 다시 시작하므로 무조건 재시도가 무해한 것은 아닙니다.
