# 외부 배포 준비 반영 기록

2026-11-16 전달용 검토안을 코드와 대조해 반영했다. 이 문서는 공개·릴리스 승인을 뜻하지 않는다.

| 제안 | 판단·반영 |
|---|---|
| 만료 키 메모리·삭제 예약 누적 | 반영. 능동 정리, 회수·사용량 갱신, 재작성 시 물리 만료 제거 후 예약 재계산 |
| 예약 합계에서 만료 키만 제외 | 그대로 채택하지 않음. 노드가 남으면 후속 SET이 새 예약 없이 갱신할 수 있으므로 물리 삭제와 함께 처리 |
| 버킷 개수만 제한하는 만료 순회 | 그대로 채택하지 않음. 긴 체인도 나눠 훑는 약한 순회·최신 노드 재확인 적용. 충돌 조회 자체의 시간 상한은 보장하지 않음 |
| 전체 용량 상한 | 반영. 기본 근사 256 MiB, 증가분 거부, 삭제·축소 허용, replay 초과는 보존+경고 |
| 인증·외부 바인딩 보호 | 반영. Bearer 토큰, 고정 길이 해시 상수 시간 비교, 최소 16바이트, 비루프백 무인증 기본 거부 |
| 헬스 체크 | 반영. 무인증 요약만 노출. journal 잠금을 기다리지 않아 멈춘 Write 뒤에서 대기하지 않음 |
| Windows 빌드 | 반영. 플랫폼 잠금 분리, 미지원 OS AOF 오류. Windows는 메모리 모드 지원 |
| 모듈·버전·Docker·CI | 원격 이름과 모듈 일치, -version, 비특권 컨테이너, OS·Go 버전 CI 추가 |
| 문서·빈 패키지 | 사용자 문서 재작성, 빈 bench/origin 제거. 기록은 docs/learning에 보존 |
| 학습·실험 테스트 삭제 | 보류. 사용자의 기존 학습 목적을 보존하고 일반 실행에서는 기존처럼 skip |
| 커밋·푸시·라이선스 승인 | 실행하지 않음. 사용자가 변경을 검토하고 팀 공개 동의를 확인할 작업 |
| 메시지 전부 영어화 | 미반영. 기존 동작·학습 문맥 보존. 클라이언트는 안정적인 code로 분기 |
| metrics·접근 로그·빈도 제한 내장·TLS·릴리스 자동화 | 선택 사항으로 미구현. 프록시·운영 안내 제공 |

## 검증 항목

- `TestExpiredKeysDoNotExhaustAOFReservations`: 64 KiB AOF 상한, 서로 다른 TTL 키 5,000개, 매 200개 재작성 후 메모리·예약 회수와 새 쓰기 확인.
- `TestDataCapacityAccounting`, `TestCapacityFailureDoesNotAppendAndReplayCanShrink`: 용량 거부 시 값·TTL·로그 보존, 삭제·축소, 더 작은 상한으로 복구.
- `TestActiveExpirationMemoryAndClose`, `TestMaintenancePanicIsContained`: 미조회 만료 회수, 종료·panic 격리·잠금 해제.
- `TestSweepConcurrentMutationReservationInvariant`: 동시 SET·GET·DELETE·정리 후 삭제 예약 불변식.
- `TestHealthDoesNotBlockBehindJournal`, `TestAuthHealthAndCapacity`, `TestDeploymentSettings`: 비차단 헬스, 인증과 용량 오류, 외부 바인딩 보호.

## 검증 결과

- 로컬 Go 1.27.2/macOS arm64: 전체 일반 테스트, 전체 레이스 검사, vet, build, gofmt 확인, diff 공백 검사 통과.
- 새 만료·용량·동시 정리·panic 관련 회귀 테스트 5회 레이스 반복 통과.
- Go 1.22 Alpine 컨테이너: 전체 일반 테스트·vet 통과.
- Windows amd64, Linux amd64, macOS arm64 전체 패키지 교차 빌드 통과. Windows 실행 자체는 검사하지 않음.
- 임시 GOBIN으로 두 CLI 로컬 설치와 `miniredis -version` 출력 dev 확인.
- Docker 이미지 실제 빌드 통과. 무토큰 공개 바인딩 기동 거부, UID 10001, 헬스·인증·CRUD·용량 거부·미조회 TTL 회수 확인.
- 전용 임시 Docker 볼륨의 AOF로 SIGTERM 종료 코드 0과 재시작 후 유효 값 보존·삭제/만료 키 부재 확인. 검사 컨테이너와 전용 볼륨은 제거함.
- README와 현재 사용자 문서의 로컬 링크 존재 확인.

GitHub CI는 추가했으나 원격에서 실행하지 않았다. 원격 `go install ...@latest`도 이 변경이 원격에 반영되기 전에는 새 변경을 검증하지 못한다. systemd·프록시 예시는 실제 호스트에서 적용·검증하지 않았다. 성능 수치·RSS 상한·무손실 보장은 이번 검사 결과에서 도출하지 않는다. 커밋·푸시·실제 외부 공개는 실행하지 않았다.


## 외부 배포 반영분 재검토 후속 수정

- 고정 탐색량을 테이블 크기·만료 비율에 따른 적응형 정리로 변경하고, 상한 거부 전 4,096 탐색 단위 회수 후 SET 상태를 재확인한다.
- 헬스 체크는 atomic 상태와 단조 경과 시간으로 정상 경합·멈춘 Write·Sync 지연·fatal을 구분한다.
- Bearer 스킴 대소문자 호환, HTTP 오류 안내 한국어 통일, 이 내부 기록의 학습 폴더 이동을 반영한다.
- PRD와 학습 기록은 보존했다. 공개 저장소의 팀 문서·라이선스 동의 여부는 이 코드 작업이 대신 승인하지 않는다.
- 추가 테스트: TestCapacityPressureReclaimsAndReservesReplacement, TestHealthStableDuringConcurrentWrites, TestHealthDetectsStalledFirstWrite, TestHealthMonotonicDirtyAgeAndSyncRecovery, TestExpirationSustainedShortTTL. 실시간 장기 검증은 -short에서 생략한다.


후속 검증 결과(로컬 Go 1.27.2, macOS arm64): 전체 일반 테스트·전체 레이스·vet·build·Windows 교차 빌드·gofmt·diff 검사 통과. 실제 시계로 각 모드에 키 50,000개, 목표 5,000개/초, TTL 50ms, 근사 데이터 상한 1 MiB를 적용한 일반 테스트에서 메모리 모드 9.991초·물리 키 705개, AOF 모드 9.993초·물리 키 746개, 두 모드 용량 거부 0건이었다. 쓰기 8개와 병행한 헬스 조회 2,000회는 오판 0건, 첫 Write를 실제 3.1초 멈추면 비정상으로 전환했다. 이는 Store 수준 회귀 검증이며 HTTP 서비스의 보장 처리량·성능 벤치마크가 아니다. `go test -short ./...`는 10초 TTL 부하와 3초 디스크 중단 검증을 생략한다.
