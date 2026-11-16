# Mini Redis

직접 구현한 해시 테이블을 사용하는 HTTP/JSON 문자열 키-값 저장소입니다. TTL, 키 단위 삭제, 동시 요청 처리, 선택적 AOF 영속성을 제공합니다. 단일 인스턴스용이며 운영 규모의 성능·장애 복구를 보증하는 제품은 아닙니다.

**Redis 프로토콜(RESP), redis-cli, Redis 클라이언트 라이브러리와 호환되지 않습니다.** HTTP 클라이언트로 사용합니다.

## 설치와 실행

소스 빌드는 Go 1.22 이상이 필요합니다. 최신 macOS에서는 최신 Go 사용을 권장합니다. 운영 바이너리는 보안 패치를 받는 Go 버전으로 빌드하세요. AOF는 Linux·macOS의 로컬 파일 시스템에서 지원하며 Windows는 메모리 모드만 지원합니다.

```bash
git clone https://github.com/AI-Together-W10-W11-T1/Mini_Redis.git
cd Mini_Redis
go build -o bin/miniredis ./cmd/miniredis
go build -o bin/aofrepair ./cmd/aofrepair
./bin/miniredis -version
./bin/miniredis
```

원격 저장소에 이 변경이 반영된 후에는 아래 설치도 가능합니다. 현재 작업 폴더의 변경이 자동으로 배포되지는 않습니다.

```bash
go install github.com/AI-Together-W10-W11-T1/Mini_Redis/cmd/miniredis@latest
go install github.com/AI-Together-W10-W11-T1/Mini_Redis/cmd/aofrepair@latest
```

기본 주소는 `127.0.0.1:8080`, AOF는 `data/appendonly.aof`입니다. `Ctrl+C`·SIGTERM으로 종료하면 마지막 Sync를 시도하며 전체 종료 대기는 5초입니다. Windows에서는 `MINI_REDIS_AOF=false`로 실행합니다.

```bash
curl -sS -X PUT 'http://127.0.0.1:8080/keys/user%3A101' \
  -H 'Content-Type: application/json' -d '{"value":"Minji","ttl_ms":30000}'
# {"ok":true}
curl -sS 'http://127.0.0.1:8080/keys/user%3A101'
# {"key":"user:101","value":"Minji","ttl_ms":...}
curl -sS -X DELETE 'http://127.0.0.1:8080/keys/user%3A101'
# {"deleted":1}
curl -sS 'http://127.0.0.1:8080/health'
# {"status":"ok","persistence":"aof"}
```

## Docker

```bash
docker build -t miniredis .
# 먼저 충분히 긴 토큰을 환경변수로 지정합니다. 실제 값은 저장소에 넣지 마세요.
export MINI_REDIS_AUTH_TOKEN="$(openssl rand -hex 32)"
docker run -d --name miniredis -p 127.0.0.1:8080:8080 \
  -e MINI_REDIS_AUTH_TOKEN -v miniredis-data:/data miniredis
curl -sS -H "Authorization: Bearer $MINI_REDIS_AUTH_TOKEN" \
  'http://127.0.0.1:8080/keys/user%3A101'
docker stop miniredis
```

컨테이너 내부는 `0.0.0.0:8080`이므로 토큰 없이는 시작을 거부합니다. 기본 실행 계정은 비특권 사용자이며 데이터는 볼륨에 저장합니다. 볼륨을 삭제하면 데이터도 삭제됩니다.

## API와 설정

| 요청 | 의미 |
|---|---|
| `PUT /keys/{key}` | `{"value":"문자열","ttl_ms":30000}` 저장. TTL 생략 시 무기한 |
| `GET /keys/{key}` | 값과 남은 TTL(밀리초, 올림) 반환. 무기한은 -1, 없음·만료는 404 |
| `DELETE /keys/{key}` | `{"deleted":1}` 또는 `{"deleted":0}` |
| `GET /health` | 정상 200, 영속성 장애·장기 기록 지연 시 503. 인증 불필요 |

키는 1~256 UTF-8 바이트이며 제어 문자와 `.`·`..` 키는 허용하지 않습니다. 키를 URL 경로 구간으로 인코딩하세요(`a/b` → `a%2Fb`). 값은 최대 65,536바이트 문자열입니다. 조회는 TTL을 연장하지 않으며 덮어쓰기는 값과 TTL을 모두 교체합니다. 전체 API와 오류·재시도 계약은 [docs/api.md](docs/api.md)를 참고하세요.

| 환경변수 | 기본값 | 의미 |
|---|---|---|
| `MINI_REDIS_ADDR` | `127.0.0.1:8080` | 대기 주소 `host:port` |
| `MINI_REDIS_AUTH_TOKEN` | 없음 | 지정 시 키 API에 Bearer 인증. 최소 16바이트 |
| `MINI_REDIS_ALLOW_UNAUTHENTICATED` | `false` | 비루프백 주소의 무인증 실행을 명시적으로 허용 |
| `MINI_REDIS_MAX_DATA_BYTES` | `268435456` | 데이터 근사 상한: 키+값+항목당 64바이트 |
| `MINI_REDIS_MAX_BODY_BYTES` | `524288` | HTTP 요청 본문 상한 |
| `MINI_REDIS_AOF` | `true` | `false`는 기존 파일도 읽지 않는 메모리 모드 |
| `MINI_REDIS_AOF_PATH` | `data/appendonly.aof` | 실행 디렉터리 기준 파일 경로 |
| `MINI_REDIS_AOF_MAX_BYTES` | 데이터 상한의 2배 (`536870912`) | AOF 파일+삭제 예약량 상한. 지정하지 않으면 데이터 상한의 2배이며 최소 `268435456` |
| `MINI_REDIS_AOF_REWRITE_MIN_BYTES` | `4194304` | 최소 재작성 증가량 기준. AOF 상한보다 작아야 함 |
| `MINI_REDIS_AOF_SYNC_INTERVAL_MS` | `1000` | 디스크 Sync 주기(10~60000). 길수록 장애 시 잃을 수 있는 구간이 늘어남 |
| `MINI_REDIS_DEBUG` | `false` | 디버그 로그. `-debug` 플래그 우선 |

데이터 상한은 **프로세스 RSS 상한이 아닙니다.** 버킷 배열·런타임·GC·재작성 버퍼 등은 별도 메모리를 사용합니다. 키가 크게 줄면 만료 정리가 한 바퀴를 마칠 때 버킷 배열도 줄입니다. 상한에서는 증가하는 쓰기를 거부하며 같은 크기·더 작은 갱신과 삭제를 허용합니다(영속성 장애 등 별도 오류는 제외). 자동 LRU/LFU 축출은 없습니다. AOF는 Base64와 삭제 예약 때문에 같은 데이터에 더 많은 공간을 쓰므로, AOF 상한을 데이터 상한의 2배보다 작게 지정하면 데이터 상한에 닿기 전에 쓰기가 `PERSISTENCE_UNAVAILABLE`로 거부될 수 있습니다. 2배는 일반적인 키·값을 위한 기본 정책이며 모든 입력에서 충분함을 보장하지는 않습니다. 만료 항목은 조회 시와 적응형 백그라운드 정리로 회수하며, 용량 거부 전에도 제한된 동기 회수를 시도합니다.

**운영 환경에서는 디버그 로그를 끄세요.** 키 이름과 경로가 기록되며 성능에도 영향을 줍니다. 외부 접속은 Bearer 토큰과 TLS 리버스 프록시를 함께 사용하세요. 내장 TLS·권한별 접근 제어·복제·클러스터는 제공하지 않습니다.

## 캐싱 데모와 성능 비교

원본 API와 소비 앱을 띄워 캐시 적중·미적중을 직접 확인할 수 있습니다. 자세한 내용은 [docs/demo.md](docs/demo.md)에 있습니다.

```bash
MINI_REDIS_AOF=false go run ./cmd/miniredis   # 터미널 1
go run ./cmd/demo                             # 터미널 2: 원본 8081, 소비 앱 8082
curl -si 'http://127.0.0.1:8082/demo/products/42' | grep -i x-cache   # MISS, 다시 호출하면 HIT
```

캐시 사용·미사용 성능 비교는 `go run ./cmd/cachebench`로 측정합니다. 사용법은 [docs/benchmark.md](docs/benchmark.md), 측정 결과는 [docs/results/prd-t4.md](docs/results/prd-t4.md)를 참고하세요.

## 영속성과 문서

AOF는 변경을 파일에 기록하고 기본 약 1초마다 Sync합니다. **성공 응답은 디스크 동기화 완료를 뜻하지 않습니다.** 장애 시 마지막 동기화 이후 변경을 잃을 수 있으며 Sync 주기는 손실 상한이 아닙니다. 이미 시작한 변경은 503 `PERSISTENCE_OUTCOME_UNKNOWN` 뒤에도 반영될 수 있습니다.

- [API·오류 코드](docs/api.md)
- [영속성 보장·백업·손상 복구](docs/persistence.md)
- [구조·동시성·만료·메모리 정책](docs/architecture.md)
- [Docker·systemd·TLS 배포](docs/deployment.md)
- [캐시 연동 방법과 경합 한계](docs/cache-aside.md)
- [캐싱 데모: 원본 API와 소비 앱](docs/demo.md)
- [성능 측정 방법](docs/benchmark.md)과 [측정 결과](docs/results/prd-t4.md)
- [설계 선택과 이유](docs/design-decisions.md)
- [검증 케이스 대응표](docs/verification.md)

과제 PRD와 단계별 학습·검수 기록은 [docs/learning](docs/learning/README.md)에 따로 보존합니다. 현재 동작의 기준은 위 문서들입니다.

## 개발

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
gofmt -l .
```

[MIT 라이선스](LICENSE)를 사용합니다.
