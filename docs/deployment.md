# 배포

## 지원 범위

단일 인스턴스 HTTP/JSON 서비스입니다. AOF는 Linux·macOS의 로컬 디스크에서 지원합니다. Windows 바이너리는 빌드되지만 AOF 실행은 명확한 오류로 거부되므로 `MINI_REDIS_AOF=false`로 실행하세요. 복제·자동 장애 조치·TLS·다중 사용자 권한은 제공하지 않습니다.

## Docker

README의 빌드·실행 명령을 사용합니다. Dockerfile은 비특권 UID 10001로 실행하고 `/data`를 볼륨으로 사용합니다. 바인드 마운트는 이 UID가 쓸 수 있도록 소유권·권한을 미리 맞춰야 합니다. 실행 계정이 읽을 수 없는 비밀 파일이나 쓰기 불가능한 AOF 경로는 시작 실패 원인이 됩니다.

기본 빌드 이미지는 `golang:1.27-alpine3.24`, 실행 이미지는 `alpine:3.24`입니다. 빌드 시 `GO_IMAGE`·`RUNTIME_IMAGE` 인자로 교체할 수 있습니다. 태그는 갱신될 수 있으므로 재현 가능한 릴리스에는 검증한 digest를 지정하고 정기적으로 패치를 반영하세요. 이미지 계열은 [Go 공식 이미지](https://hub.docker.com/_/golang)와 [Go 릴리스](https://go.dev/dl/)를 확인합니다.

```bash
docker build --build-arg VERSION=v0.1.0 -t miniredis:v0.1.0 .
# 로컬 검증은 호스트 루프백에만 포트를 공개
export MINI_REDIS_AUTH_TOKEN="$(openssl rand -hex 32)"
docker run -d --name miniredis -p 127.0.0.1:8080:8080 \
  -e MINI_REDIS_AUTH_TOKEN -v miniredis-data:/data miniredis:v0.1.0
docker inspect --format '{{.State.Health.Status}}' miniredis
docker stop miniredis
```

컨테이너 내부 비루프백 바인딩에는 토큰이 필요합니다. `MINI_REDIS_ALLOW_UNAUTHENTICATED=true`는 격리된 환경에서 의도적으로 사용할 때만 설정하세요. 환경변수는 Docker 관리자에게 보일 수 있습니다. 현재 토큰 파일 읽기 옵션은 없으므로 조직의 비밀 관리·접근 권한 정책을 따르세요.

## Linux systemd 예시

먼저 전용 `miniredis` 계정, 소유권이 맞는 `/var/lib/mini-redis`, `/usr/local/bin/miniredis`를 준비합니다. `/etc/mini-redis.env`는 서비스 관리자가 배치하고 비밀 토큰에 맞는 읽기 권한을 제한합니다.

```ini
# /etc/mini-redis.env (실제 토큰으로 교체; 저장소에 커밋하지 않음)
MINI_REDIS_ADDR=127.0.0.1:8080
MINI_REDIS_AUTH_TOKEN=replace-with-a-long-random-secret
MINI_REDIS_AOF_PATH=/var/lib/mini-redis/appendonly.aof
MINI_REDIS_DEBUG=false
```

```ini
# /etc/systemd/system/miniredis.service
[Unit]
Description=Mini Redis HTTP KV store
After=network.target

[Service]
User=miniredis
Group=miniredis
WorkingDirectory=/var/lib/mini-redis
EnvironmentFile=/etc/mini-redis.env
ExecStart=/usr/local/bin/miniredis
Restart=on-failure
RestartSec=5
TimeoutStopSec=10
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/mini-redis

[Install]
WantedBy=multi-user.target
```

관리자가 `systemctl daemon-reload` 후 서비스를 시작합니다. 운영 디스크와 메모리 제한은 데이터 근사 상한 외에 재작성·Go 런타임 여유를 포함해 설정하세요.

## TLS와 요청 제한

서버는 루프백에 두고 TLS 리버스 프록시만 외부에 공개하세요. 예를 들어 Caddy는 다음처럼 구성할 수 있습니다. 도메인과 인증서 발급 조건은 실제 환경에 맞춰야 합니다.

```text
cache.example.com {
    reverse_proxy 127.0.0.1:8080
}
```

nginx의 요청 제한 예시입니다. `limit_req_zone`은 `http` 블록, `limit_req`는 프록시 `location`에 두고 기존 TLS 서버 설정과 결합합니다. IP·요청량은 실제 클라이언트와 프록시 신뢰 구성을 반영해 조정하세요.

```nginx
# http 블록
limit_req_zone $binary_remote_addr zone=miniredis:10m rate=50r/s;
# TLS server 블록 안의 location
location / {
    limit_req zone=miniredis burst=100 nodelay;
    proxy_pass http://127.0.0.1:8080;
    proxy_set_header Authorization $http_authorization;
}
```

`/health`는 상세 정보 없이 인증 없이 노출합니다. 정상적인 journal 잠금 경합은 장애로 보지 않고, fatal 오류나 장기 미동기화·Write 지연을 보고합니다. 프로브 정책은 연속 실패와 실제 복구 절차를 반영해 설정하세요. 데이터 근사 사용량·디스크 여유·로그 수집은 외부 모니터링을 병행합니다. 내장 `/metrics`와 자동 백업은 없습니다.

정상 종료 후 AOF 백업과 복구 시연을 수행하고, 캐시가 원본을 대신하는 유일한 보관소인지 여부는 애플리케이션에서 결정하세요. [영속성 보장·복구](persistence.md)를 참고합니다.
