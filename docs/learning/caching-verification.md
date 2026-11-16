# 외부 앱의 캐싱 사용 검증

## 범위

2026-11-14, 현재 mini-Redis HTTP API를 외부 앱이 캐시로 사용할 수 있는지 검증했다. 제품 구현은 변경하지 않고 `internal/demo/demo_test.go`만 추가했다. 원본·소비 앱은 테스트 전용 HTTP 핸들러이며 실행 가능한 제품 앱은 아직 아니다.

같은 테스트 안에서 원본·mini-Redis·소비 앱을 각각 `httptest.NewServer`로 띄웠다. 소비 앱은 Store를 직접 호출하지 않고 HTTP GET·PUT으로 mini-Redis를 사용한다. 원본 변경과 캐시 삭제도 HTTP로 요청했다. 고정 포트·기존 실행 서버·별도 프로세스를 사용하지 않는다.

```text
테스트 클라이언트 → 소비 앱
                     ├─ 캐시 HIT → mini-Redis의 저장 응답 반환
                     └─ 캐시 MISS → 원본 GET → mini-Redis PUT → 응답
```

공유 HTTP 클라이언트는 요청 제한 3초, MaxIdleConnsPerHost=20, MaxIdleConns=60을 사용한다. 응답 본문은 모두 읽고 닫는다. TTL은 가짜 시계를 전진시켜 검증한다. 디버그는 끄고, 성능 측정·인위적 원본 지연은 적용하지 않았다.

## 결과

| 테스트 | 확인한 사실 | 결과 |
|---|---|---|
| TestCacheMissHitAndKeyIsolation | 첫 조회 MISS·원본 1회, 다음 HIT·추가 0회. 공개 API로 저장된 상품 JSON·TTL 확인. 두 상품의 캐시 독립 | PASS |
| TestCacheExpiryAndInvalidation | TTL 만료 정각에 MISS·변경 상품 반영. 원본 PUT → 캐시 DELETE → 다음 조회 MISS·새 값 저장 → 이후 HIT | PASS |
| TestCacheBypassPreservesExistingCache | off 요청 2회는 원본도 2회 호출. 기존 캐시 값·남은 TTL 그대로 유지 | PASS |
| TestCacheDoesNotStoreFailedOriginResponses | 원본 404·500은 캐싱하지 않음. 원본 복구 후 MISS로 정상 저장 | PASS |

원본 호출 수는 원본의 `/stats`를 HTTP로 조회해 확인했다. 각 테스트마다 독립 데이터·Store·서버를 생성하고 끝나면 정리한다.

## 재실행

프로젝트 루트에서 실행한다. 로컬 임시 HTTP 포트를 열 수 있는 환경이 필요하다.

```bash
go test ./internal/demo -run '^TestCacheMissHitAndKeyIsolation$' -count=1 -v
go test ./internal/demo -run '^TestCacheExpiryAndInvalidation$' -count=1 -v
go test ./internal/demo -run '^TestCache(BypassPreservesExistingCache|DoesNotStoreFailedOriginResponses)$' -count=1 -v
```

모두 PASS가 예상 결과다. 전체 캐싱 검증은 `go test ./internal/demo -count=1 -v`로 실행한다.

추가 검증 결과: `go test -race ./... -count=1`, `go vet ./...` 모두 종료 코드 0. 레이스 보고 없음. 실행 환경은 Go 1.27.2 darwin/arm64이며 검증 도구의 빌드 캐시는 `/private/tmp/mini-redis-go-build`를 사용했다.

## 아직 남은 작업

| 우선순위 | 작업 | 완료 기준 |
|---|---|---|
| 필수 | 실행 가능한 원본 API·소비 앱 | internal/origin·internal/demo 제품 코드와 cmd/origin·cmd/demo. 상품 조회·변경·호출 수, 캐시 on/off·HIT/MISS 지원 |
| 필수 | 실제 앱으로 외부 기능 테스트 연결 | 이번 테스트용 앱을 실제 핸들러로 교체해 같은 캐싱 시나리오 재검증. 같은 테스트의 httptest 서버 방식 유지 |
| 필수 | 성능 비교 도구 | internal/bench·cmd/bench, 캐시 사용/미사용 비교. 기본 상품 100개·동시 요청 20개, 준비 3초·측정 10초·1회 |
| 필수 | 성능 결과 기록·최종 안내 | docs/results에 CSV·실행 환경·해석. 전체 실험보다 긴 TTL로 사전 웜 캐시 준비, 요청 지연·처리량·오류·적중률·원본 호출 수 기록. README 실행·검증 안내 |
| 선택 | 추가 운영 정책 | 주기적 만료 정리·메모리 한도·축출·점진적 리사이징·영속성 등. 기존 승인 범위를 바꾸는 경우 먼저 결정 |

이번 결과는 HTTP를 통한 캐싱 기능의 사용 가능성을 확인한 것이다. 실무 처리량·장시간 안정성·메모리 상한을 검증한 결과는 아니다. 남은 구현은 이번 작업에서 진행하지 않았다.
