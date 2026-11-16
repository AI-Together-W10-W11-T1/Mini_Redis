# 1단계: 해시 테이블을 실행하며 이해하기

이 단계는 ③ 해시 테이블만 다룬다. FNV-1a 64비트, 버킷 16개, 체이닝, 적재율 0.75 초과 시 2배 일괄 확장이다. 저장 규칙·HTTP·전역 락은 아직 없다. 따라서 Redis CLI나 curl 대신 **설명이 출력되는 Go 테스트**를 실행한다. 서버·기존 Redis에 접속하거나 데이터를 변경하지 않는다.

## 1. 파일과 실행 준비

- `internal/hashtable/table.go`: Record, 해시, Put·Get·Delete, 개수·용량·확장.
- `internal/hashtable/table_test.go`: 직접 실행할 테스트와 격리된 실패 실험.
- Record의 ExpiresAt은 저장·교체·확장 때 보존만 한다. 지금은 만료 시각이 지나도 Table.Get이 값을 반환한다. 만료 판단은 2단계 Store의 책임이다.
- Table 자체는 동시성 안전하지 않다. 다음 단계에서 같은 Store의 전역 락이 모든 접근을 보호한다.

프로젝트 루트에서 실행한다.

```bash
go test ./...
```

예상 결과는 hashtable 패키지의 `ok`, 다른 빈 패키지의 `[no test files]`다. 아래 시간은 실행마다 다르다.

```text
ok      mini-redis/internal/hashtable    ...s
```

## 2. 직접 입력할 명령 4개

각 명령은 독립된 새 테이블에서 실행된다. `-v`는 설명 출력, `-count=1`은 캐시된 테스트 결과 대신 실제 재실행이다. MINI_REDIS_DEBUG는 이 단계의 테스트가 읽어 로거로 전달한다. 코어가 환경변수를 직접 읽는 구조는 아니다.

### ① 키가 어느 버킷으로 가는가?

```bash
go test ./internal/hashtable -run '^TestHashFNV1aKnownVectors$' -count=1 -v
```

출력 중 다음을 확인한다.

```text
key="user:101" hash=6856737180763075376 bucket(16)=0
key="product:1001" hash=11619022900221879904 bucket(16)=0
PASS
```

두 해시값은 다르지만 16으로 나눈 나머지는 같다. **충돌은 해시값 전체가 같을 때만 생기는 것이 아니다.** 빈 문자열 해시 벡터는 함수 정확성 검증용이며 HTTP에서 빈 키를 허용한다는 뜻은 아니다.

### ② 저장·조회·갱신·삭제·빈 값을 확인하기

```bash
MINI_REDIS_DEBUG=true go test ./internal/hashtable -run '^TestTableSetGetUpdateAndEmptyValue$' -count=1 -v
```

실제로 다음 과정을 실행한다.

1. 없는 키 조회·삭제는 부재를 반환.
2. user:101=Minji 저장 후 조회.
3. 같은 키를 Jisoo로 교체하되 키 개수는 1 유지.
4. 빈 문자열 값은 found=true로 조회.
5. 조회해 받은 복사본을 바꿔도 저장된 값은 불변.
6. 첫 삭제는 성공, 두 번째 삭제는 대상 없음.

예상 설명 출력:

```text
INSERT/GET: user:101=Minji size=1
UPDATE: user:101=Jisoo size=1 (만료 정보도 새 Record로 교체)
EMPTY/MISSING: 빈 값은 found=true, 삭제한 키는 found=false
PASS
```

### ③ 충돌한 키가 서로 영향을 주는지 확인하기

```bash
MINI_REDIS_DEBUG=true go test ./internal/hashtable -run '^TestTableCollisionDeletePositions/real_fnv_bucket_collision$' -count=1 -v
```

user:101=Minji와 product:1001=Keyboard가 같은 버킷 0에 들어간다. 새 항목은 연결의 앞에 붙인다.

```text
버킷 0 → [product:1001 | Keyboard] → [user:101 | Minji]
```

user:101을 갱신하고 삭제해도 product:1001은 Keyboard로 남아 있어야 한다. 로그에서 첫 후보가 다른 키라면 건너뛰고 다음 후보를 확인하는 과정을 본다.

### ④ 13번째 키에서 확장이 발생하는지 확인하기

```bash
MINI_REDIS_DEBUG=false go test ./internal/hashtable -run '^TestTableResizeThresholdAndPreservation$' -count=1 -v
```

```text
BEFORE: 12키·16버킷. 기존 키 갱신은 확장 없음
AFTER: 13번째 새 키 → 32버킷으로 확장
PRESERVED: 200개 값·만료 정보 보존, capacity=512
PASS
```

12/16=0.75에서는 유지하고, 13번째 새 키가 들어가면 확장한다. 이후 200키까지 넣어 여러 번 확장한 뒤 모든 값·만료 정보를 확인한다. 상세 로그도 보려면 같은 명령의 MINI_REDIS_DEBUG를 true로 바꾼다. 전체 과정이 보여 출력량이 많다.

## 3. 내부 디버그 로그 읽기

실제로 확인한 로그의 일부다. 상세 로그는 stderr, 테스트 설명은 테스트 출력으로 표시된다.

```text
level=DEBUG msg=HASH op=put key=user:101 hash=6856737180763075376 bucket=0
level=DEBUG msg=INSERT key=user:101 bucket=0 size=1 capacity=16
level=DEBUG msg=COLLISION bucket=0 new_key=product:1001 existing_key=user:101
level=DEBUG msg=COMPARE requested=user:101 stored=product:1001 match=false
level=DEBUG msg=COMPARE requested=user:101 stored=user:101 match=true
level=DEBUG msg=DELETE key=user:101 deleted=true size=1
```

- HASH: 키의 실제 해시값과 버킷 번호.
- COLLISION: 기존 항목이 있는 버킷에 다른 키 추가.
- COMPARE: 현재 후보가 찾는 키와 같은지 판단.
- INSERT·UPDATE·DELETE: 새 삽입·기존 값 교체·삭제와 물리 키 수.

확장 시에는 다음을 볼 수 있다.

```text
level=DEBUG msg=RESIZE old_capacity=16 new_capacity=32 size=12
level=DEBUG msg=MOVE key=item:2 old_bucket=2 new_bucket=18
```

재배치는 기존 항목을 옮기는 일이므로 개수를 늘리지 않는다. 새 배열에 연결하기 전에 기존 next 참조를 보관해야 항목을 잃지 않는다.

디버그는 환경변수 생략 또는 false로 끈다. Table 생성 시 디버그 활성 여부를 한 번 확정한다. 실행 중 로그 수준 변경은 지원하지 않는다. 각 로그 호출 전에 조건을 검사하므로 off에서는 로그 인자 준비를 생략한다. 이는 전체 연산의 메모리 할당이 없다는 뜻은 아니다. off에서 상세 로그가 없고 on에서 HASH·COMPARE·RESIZE·MOVE가 보이는 것을 확인했다. **성능 측정에서는 끈다.** 제품 서버의 -debug 플래그는 실행 진입점을 만드는 단계에서 연결한다.

## 4. 일부러 깨뜨려보기: 실제 키 비교를 없애면?

다음 실험은 `_test.go` 안에만 있는 잘못된 조회를 실행한다. 정상 Table.Get이나 저장 코드를 변경하지 않으며, 켜지 않으면 기본 테스트에서 건너뛴다.

```bash
MINI_REDIS_DEBUG=true MINI_REDIS_EXPERIMENT=skip-key-check go test ./internal/hashtable -run '^TestExperimentWithoutKeyComparison$' -count=1 -v
```

실험은 두 키의 전체 해시를 **테스트에서만 42로 강제**한다. 같은 버킷뿐 아니라 해시값까지 같아도 실제 키 비교가 필요하다는 것을 보여 주기 위해서다. 먼저 정상 Get이 Minji를 반환함을 확인한 뒤, 잘못된 조회는 해시만 비교하고 첫 항목 Keyboard를 반환한다.

예상 결과:

```text
의도한 실패: 키 비교 제거 → user:101 조회가 "Keyboard" 반환, 정답은 "Minji"
--- FAIL: TestExperimentWithoutKeyComparison
FAIL
```

**이 명령은 실패·종료 코드 1이 기대 결과다.** 실험용 오류를 정상 구현의 오류로 해석하지 않는다. 원래대로 돌아가려면 다음 명령을 실행하면 된다. 일회성 환경변수이므로 별도 코드 복구는 필요 없다.

```bash
go test ./...
```

## 5. 테스트 코드와 확인 결과

골격 단계에서는 기존 미승인 구현을 정리하고 Homebrew Go 설치·버전을 확인했다. 모듈 `mini-redis`·Go 언어 기준 1.22.0과 내부 패키지 7개의 빈 패키지 파일을 생성해 빌드를 통과했다. 1단계에서는 해시 테이블·필수 단위 테스트 4개·테스트 전용 실패 실험을 구현했다.

리뷰 반영: FNV-1a 64비트는 고정 초기값에서 시작해 UTF-8 바이트마다 XOR → 소수 곱셈 순서로 직접 계산한다. 기존 해시 테스트 벡터와 동작 기대값은 유지한다. 디버그 비활성 시 로그 인자 준비를 생략하며, 설계 문서에는 계약과 단계별 진행 규칙만 남긴다.

테스트 코드는 `internal/hashtable/table_test.go`에 있다.

| 테스트 | 검증 |
|---|---|
| TestHashFNV1aKnownVectors | 알려진 해시값·버킷 계산 |
| TestTableSetGetUpdateAndEmptyValue | 신규·교체·부재·빈 값·복사본·개수 |
| TestTableCollisionDeletePositions | 실제 버킷 충돌, 전체 해시 충돌, 첫·중간·끝·없는 키 삭제·재삽입 |
| TestTableResizeThresholdAndPreservation | 임계값·갱신 시 미확장·반복 확장 후 200항목 보존 |
| TestExperimentWithoutKeyComparison | 기본은 SKIP. 명시적으로 켜면 의도한 FAIL |

전체 확인 명령:

```bash
go test ./...
go vet ./...
go build ./...
```

확인 결과: 정상 테스트·vet·build 통과. 디버그 on/off 확인. 별도 실패 실험은 예상대로 종료 코드 1. 실행 환경은 Go 1.27.2 darwin/arm64, 검증 도구의 빌드 캐시만 /private/tmp/mini-redis-go-build를 사용했다.

리뷰 반영 후 재검증(2026-11-14): 아래 네 명령 모두 종료 코드 0으로 통과했다. 기존 테스트 코드와 기대값은 변경하지 않았다. 디버그 true는 DEBUG 로그 1,723줄, false는 0줄이었다. 의도적 실패 실험은 활성화하지 않아 SKIP이다.

```bash
go test ./... -count=1
go vet ./...
MINI_REDIS_DEBUG=true go test ./internal/hashtable -count=1 -v
MINI_REDIS_DEBUG=false go test ./internal/hashtable -count=1 -v
```

저장 규칙·동시성 보호·만료 판단·HTTP·소비 앱·bench는 아직 구현하지 않았다. 다음 단계는 사용자가 “다음”이라고 요청한 뒤 진행한다.

## 커밋 명령

현재 폴더가 Git 저장소가 아니라면 구조 커밋부터 만든다. 실제 커밋은 사용자가 실행한다.

```bash
git init
git add .gitignore docs/design.md go.mod internal/*/doc.go
git commit -m "chore: initialize project structure and design"
```

이어서 1단계 구현을 커밋한다.

```bash
git add internal/hashtable/table.go internal/hashtable/table_test.go docs/step1-hashtable.md
git commit -m "feat: implement step 1 hash table and learning tests"
```
