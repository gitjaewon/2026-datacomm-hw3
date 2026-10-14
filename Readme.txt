HW#3 Fault-Tolerant 협업 Worker Ring
====================================================================

1. 조원
--------------------------------------------------------------------
조 이름 :
이름 / 학번 / 역할
  -
  -
  -


2. 배포 정보
--------------------------------------------------------------------
원격 호스트 : AWS (인스턴스 유형:            / OS:            )
             공인 IP는 가림. 보안 그룹에서 TCP 8000 허용
로컬 PC     : Windows, Chaos Launcher 1개 + Worker 10개 (Worker i의 Peer 포트 = 7000 + i)
언어        : Java 21 (표준 라이브러리만 사용)
로그 시간대 : KST (Asia/Seoul), 형식 [HH:MM:SS.mmm]
seed        : 42
step-delay  : 100 ms

실행 명령
  [원격 호스트] Coordinator
    javac -encoding UTF-8 -d out src/*.java
    java -cp out Coordinator --host 0.0.0.0 --port 8000 --log logs/Coordinator.txt

  [로컬 PC] Launcher (Worker 10개를 직접 실행하고 장애를 주입)
    build.bat            (또는 javac -encoding UTF-8 -d out src\*.java)
    java -cp out Launcher --coord <공인IP>:8000 --workers 10 --peer-ip 127.0.0.1 --peer-base 7000 --step-delay 100 --seed 42 --log-dir logs

  Launcher가 내부적으로 실행하는 Worker 명령
    java -cp out Worker --id i --coord <공인IP>:8000 --peer-ip 127.0.0.1 --peer-port 700i --step-delay 100 --log-dir logs

  실행이 끝난 뒤 정합성 확인
    java -cp out LogChecker logs        (결과는 logs/check_result.txt)


3. 프로그램 구성
--------------------------------------------------------------------
src/Coordinator.java  원격 Coordinator
src/Worker.java       Worker (프로세스 10개)
src/Launcher.java     Chaos Launcher
src/Primes.java       구간 에라토스테네스의 체 (java -cp out Primes 로 검증값 확인)
src/Log.java          공통 로그 형식
src/LogChecker.java   로그 12개를 읽어서 정합성 확인과 crash별 지표 계산

3-1. Coordinator Thread 구성 (실행 중 Thread를 새로 만들지 않음)
  Main (1)            초기화, Thread 생성, RING / START 전송, 종료 대기 → VERIFY, BYE, join
  Listener (1)        Selector로 연결 수락과 10개 Socket 감시, 줄 단위 파싱.
                      HB는 직접 lastHB 갱신, 나머지는 (Socket, 메시지)로 Request Queue에 넣음
  Dispatcher (6, 고정) Request Queue에서 HELLO, RING_OK, REQ, RESULT, REJOIN을 꺼내 처리
  Failure Detector (1) START 이후 1초마다 lastHB 검사 → SUSPECT / DEAD → TAKEOVER
  Monitor (1)         5초마다 POOL 로그 (Request Queue 길이, PENDING/ASSIGNED/DONE 수, ALIVE 목록)

  공유 데이터와 보호
    Task Table [0..999]   Task마다 ReentrantLock (state, owner, epoch, result, commit_count)
    Worker Table [1..10]  Lock 1개 wt_lock (state, lastHB, rejoin_count, Socket, peer 주소, ring_ok)
    PENDING Queue         wt_lock 구간 안에서만 poll
    Request Queue         Lock + Condition(notEmpty)
    done_count, total     Lock + Condition(allDone), 1000번째 Commit이 Main을 깨움
    Socket 송신           Socket마다 send Lock
  Lock 순서: Worker Table Lock → Task Lock(번호 오름차순). Lock 안에서는 상태만 바꾸고 send와 로그는 Lock을 푼 뒤에 한다.
  모든 대기는 Condition Variable로 하고 while로 조건을 다시 확인한다.
  FD와 Monitor의 1초 / 5초 주기도 Condition의 awaitNanos로 기다려서 종료 시 바로 깨어난다.

3-2. Worker Thread 구성
  Control Recv (main thread)  Coordinator 메시지 처리
  Compute                     로컬 Queue에서 Task를 꺼내 step 1~10 계산, step마다 CKPT, 끝나면 RESULT
  Heartbeat                   RUNNING일 때만 1초마다 HB, 10초마다 CKPT 송수신 요약 로그
  Peer Server                 Peer 포트 accept. 연결마다 읽기 Thread 1개
                              (첫 메시지 PEER i → 선행 Worker의 CKPT 연결, LAUNCHER → Launcher 제어 연결)
  공유 데이터: state + crash_gen + stopping (stateLock + Condition), 로컬 Task Queue + requested + no_task
              (qLock + Condition), Checkpoint Store (storeLock)
  Lock 순서: qLock → stateLock → storeLock

3-3. Chaos Launcher
  Worker 10개를 독립 프로세스로 실행하고 각 Peer 포트에 제어 연결(첫 메시지 LAUNCHER, 거절 시 0.5초 후 재시도).
  READY 10개를 받은 시각을 t0로 하고 seed 기반 java.util.Random으로 장애를 주입한다.
    victim        그 순간 RUNNING인 Worker 중 무작위
    첫 crash      t0 + 10~20초, 이후 직전 CRASH + 5~15초
    동시 crash    CRASHED + REJOINING이 2개면 REJOINED까지 대기, 그 시각부터 간격을 새로 뽑음
    downtime D    10~15초 후 RESTART
    t0 + 80초 이후 새 crash 없음, 10개 모두 EXITED면 요약 표를 남기고 종료
  스케줄 루프는 다음 RESTART 시각과 다음 CRASH 시각 중 가까운 쪽까지 Condition으로 기다린다.

3-4. 메시지 형식 (명세 그대로, 줄바꿈 \n 단위)
  W→C  HELLO i ip port / RING_OK i / HB i / REQ i / RESULT k e v / REJOIN i
  C→W  RING j ip port / START / ASSIGN k 1 / NO_TASK / TAKEOVER k e from=d / ACK k /
       REJECT_STALE k reason / WELCOME / RETRY_LATER / BYE
  W→W  PEER i / CKPT k e s partial
  L↔W  LAUNCHER / CRASH / RESTART / READY i / REJOINED i
  Coordinator는 메시지 안의 번호가 아니라 도착한 Socket으로 보낸 Worker를 판단한다(HELLO 때 Socket에 번호 등록).
  TCP는 바이트 스트림이므로 Socket마다 버퍼를 두고 \n 단위로 자른다.

3-5. 컴파일 / 실행
  JDK 17 이상(개발은 JDK 21). 컴파일은 build.bat 또는 build.sh.
  개발용 옵션
    --step-delay 5         빠른 정상 실행 확인
    Launcher --no-chaos    장애 주입 끄기
    Launcher --manual      콘솔에 "crash 4", "restart 4" 입력으로 수동 장애
    Coordinator --host 127.0.0.1  로컬 한 대에서 실행
  제출용 정식 실행은 원격 구성 + --step-delay 100 + Launcher 기본 규칙(seed 42).


4. 장애 처리 설계
--------------------------------------------------------------------
4-1. Heartbeat / Failure Detector
  Worker는 RUNNING일 때만 1초마다 HB i를 보낸다. Listener가 HB를 받으면 바로 lastHB를 갱신하고 SUSPECT면 ALIVE로 돌린다.
  START를 보낸 시각을 모든 lastHB의 초기값으로 두고, FD는 START 이후부터 1초마다 검사한다.
    3초 이상 HB 없음 → SUSPECT (로그만)
    5초 이상 HB 없음 → DEAD
  한 번의 검사에서 새로 DEAD가 된 Worker들은 먼저 모두 DEAD로 표시하고 번호순으로 인계한다.
  DEAD Worker의 HB는 무시한다. Coordinator는 Launcher와 통신하지 않는다.

4-2. Checkpoint
  Worker i는 step s(1~9)를 끝낼 때마다 고정 Buddy(i mod 10 + 1)에게 CKPT k e s partial을 보낸다.
  Buddy는 Task별로 (epoch, step)이 사전식으로 더 큰 것만 저장한다. crash 중인 Buddy는 받은 CKPT를 버린다.
  CKPT 전송 오류는 WARN만 남기고 계산은 계속한다. Store 항목은 지우지 않는다.

4-3. Takeover
  DEAD Worker d가 owner인 모든 ASSIGNED Task(전에 인계받은 Task 포함)를 링 순서 d+1, d+2, ...에서
  처음 만나는 ALIVE Worker b에게 넘긴다. DEAD와 SUSPECT는 건너뛰고, ALIVE가 없을 때만 첫 SUSPECT에게 넘긴다.
  Task마다 Task Lock 안에서 epoch + 1, owner = b로 바꾸고, Lock을 모두 푼 뒤 TAKEOVER k e from=d를 보낸다.
  b는 Store에 k가 있으면 (k, e, 저장 step + 1, 저장 partial), 없으면 (k, e, 1, 0)을 로컬 Queue에 넣는다.

4-4. Epoch / Commit
  epoch는 Coordinator만 바꾼다(ASSIGN = 1, TAKEOVER마다 +1). Worker는 받은 e를 그대로 CKPT와 RESULT에 쓴다.
  RESULT k e v 판정 (도착한 Socket = 보낸 Worker)
    1) 보낸 Worker가 DEAD → REJECT_STALE k DEAD   (Worker Table Lock으로 확인)
    2) Task k가 이미 DONE → REJECT_STALE k DONE   (2~4는 Task k의 Lock 안에서 확인과 변경을 한 번에)
    3) e ≠ epoch 또는 보낸 Worker ≠ owner → REJECT_STALE k EPOCH
    4) 그 외 → DONE, result[k] = v, commit_count + 1, ACK k
  REQ 처리도 Worker Table Lock 안에서 "DEAD 아님 확인 + PENDING poll + Task 배정"을 한 번에 한다(고아 Task 방지).

4-5. Rejoin
  RESTART를 받으면 REJOINING으로 바꾸고 REJOIN i를 보낸다. Coordinator는 DEAD일 때만 ALIVE, lastHB = 지금,
  rejoin_count + 1 후 WELCOME, 아직 DEAD 판정 전이면 RETRY_LATER(Worker는 2초 후 아직 REJOINING이면 재전송).
  WELCOME을 받으면 RUNNING, HB 재개, Launcher에 REJOINED i, 로컬 Queue가 비었으므로 REQ로 새 Task를 받는다.
  재합류한 Worker는 이전 Task를 모르고, 모르는 Task의 ACK / REJECT_STALE은 무시한다.

4-6. Worker의 crash 처리 (가상 crash)
  CRASH를 받으면 state = CRASHED, crash_gen + 1, 로컬 Queue, REQ 대기 플래그, NO_TASK 플래그, Checkpoint Store를 모두 비운다.
  TCP 연결은 닫지 않는다. CRASHED 동안은
    - HB Thread와 Compute Thread는 state != RUNNING 조건으로 Condition 대기
    - Coordinator 메시지는 읽고 버림(BYE만 처리), CKPT도 읽고 버림, Launcher 명령은 RESTART만 처리
  Compute는 Task를 꺼내는 순간의 crash_gen을 기억하고, step마다 그리고 CKPT / RESULT를 보내기 직전에
  state == RUNNING && crash_gen == 기억한 값인지 다시 확인한다(send_if_alive). 그래서 crash 이전 Task의
  CKPT나 RESULT가 crash 중이나 rejoin 후에 새어 나가지 않는다.

4-7. 설계 선택 사항
  RING / START 전송      Main Thread
  REQ 보내는 시점        RESULT 직후 (로컬 Queue가 비는 즉시)
  합계 계산              종료 시 Σ result[k]로 VERIFY + Commit마다 누적한 total도 같은지 확인
  POOL 로그              Monitor Thread
  소수 세기              구간 에라토스테네스의 체, 10000 이하 소수 1229개는 시작 시 한 번
  설정                   모두 실행 인자 (하드코딩 없음)
  CKPT 로그              10초마다 송수신 건수 요약, TAKEOVER의 재개 step은 매번 기록


5. 인계받은 Task와 진행 중 Task의 처리
--------------------------------------------------------------------
Compute Thread는 하나이므로 두 Task를 동시에 계산하지 않는다.
TAKEOVER를 받으면 Control Recv가 로컬 Queue 뒤에 한 줄 추가할 뿐이고, Compute는 진행 중이던 Task를 끝까지
계산해 RESULT를 보낸 다음 Queue에서 인계 Task를 꺼내 CKPT 다음 step부터 이어서 계산한다(진행 중 Task 먼저).
REQ는 Queue가 완전히 비었을 때만 보낸다. 인계 Task의 CKPT와 RESULT는 새 epoch로 보낸다.


6. crash별 지표와 정합성 확인
--------------------------------------------------------------------
(정식 실행 후 Coordinator.txt 마지막 VERIFY 요약과 logs/check_result.txt 내용을 여기에 붙인다)

처리량 / 실행 시간 :
# worker lastHB DEAD detect takeover(to) recover rejoin 1st_task CKPT 절약


정합성 확인 방법 (LogChecker)
  1) Coordinator.txt의 COMMIT 줄에서 서로 다른 Task 1000개, 같은 Task 두 번 Commit 0건
  2) COMMIT 값의 합 = 5761455 (Coordinator VERIFY에서도 Σ result[k]와 누적 total을 비교)
  3) Launcher.txt의 CRASH 수 = Coordinator DEAD 수 = REJOIN 수, CRASH와 DEAD는 실제 시각으로 짝지음
  4) DEAD 로그의 dead_now 최대 크기 ≤ 2
  5) crash마다 REJOIN 이후 "first task after rejoin" ASSIGN 존재
  6) Coordinator COMMIT 수 = 1000, Worker ACK 수 ≤ 1000
  CKPT 절약률 = crash별 Σ(재개 step − 1) ÷ (인계 Task 수 × 10). Worker 로그의 TAKEOVER 재개 step으로 계산하고,
  재개 로그가 없는(CRASHED 상태에서 버려진) TAKEOVER는 0으로 센다.

결과 :


7. 추가 사항 / 한계
--------------------------------------------------------------------
- Worker는 CRASH를 받았을 때 진행 중이던 step의 계산(최대 step-delay)을 마저 기다리지만, 그 뒤 crash_gen이 달라서
  결과를 버리고 아무것도 보내지 않는다.
- Launcher 로그의 t= 값은 t0(READY 10개) 기준이고 Coordinator 로그의 lastHB / 지표 시각은 START 기준이다. 두 로그의
  대조는 실제 시각(HH:MM:SS.mmm)으로 한다.
-
