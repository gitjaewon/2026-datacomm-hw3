HW#3 데이터통신과제

1. 조원
  조: G4
  이름 / 학번 / 역할
  김재원 / 20213086 / Coordinator(Listener·Dispatcher·Failure Detector), 원격 배포, 영상
  윤도훈 / 20223122 / Worker(Compute·Checkpoint·Rejoin), 로그, LogChecker
  김치헌 / 20223097 / Chaos Launcher, 소수 계산(Primes)


2. 배포 정보
  Coordinator: AWS EC2 t3.micro (ap-northeast-2, 서울), Ubuntu 26.04 LTS, OpenJDK 21
  Worker 10개 + Chaos Launcher: 로컬 PC (Windows 11, Java 21) 한 대
  시간대: KST
  seed: 11 (이웃 Worker 두 개가 동시에 crash하는 경우가 나와서 선택), step-delay: 100ms


3. 구성
  Coordinator: Main 1(시작/종료·VERIFY), Listener 1(Selector로 10개 소켓 감시·파싱·HB 처리),
               Dispatcher 6(Request Queue에서 꺼내 처리), Failure Detector 1, Monitor 1(POOL·HB 요약 로그)
               실행 중 스레드 추가 생성 없음.
  Worker:      Control Recv 1(Coordinator 메시지), Compute 1(step 계산·CKPT·RESULT), Heartbeat 1, Peer Server 1(연결 수락)
               Peer Server가 앞 Worker의 CKPT 연결과 Launcher 연결마다 읽기 스레드를 하나씩 둠.
  Launcher:    Worker 10개 실행, 무작위 CRASH / RESTART 주입
  기타:        Primes(구간 에라토스테네스의 체), Log, LogChecker(로그 정합성 확인)

  메시지 형식 (한 줄 = 한 메시지, 명세 그대로)
    Worker -> Coordinator:   HELLO i ip port / RING_OK i / HB i / REQ i / RESULT k e v / REJOIN i
    Coordinator -> Worker:   RING j ip port / START / ASSIGN k 1 / NO_TASK / TAKEOVER k e from=d / ACK k /
                             REJECT_STALE k DEAD|DONE|EPOCH / WELCOME / RETRY_LATER / BYE
    Worker -> Buddy:         PEER i / CKPT k e s partial
    Launcher <-> Worker:     LAUNCHER / CRASH / RESTART / READY i / REJOINED i
  로그 형식은 AllDefinedLogs.txt 참고.


4. 컴파일 및 실행 (JDK 17 이상)
  Linux (Coordinator, EC2)
    javac -encoding UTF-8 -d out src/*.java
    java -cp out Coordinator --host 0.0.0.0 --port 8000 --log logs/Coordinator.txt

  Windows (Launcher, 로컬 PC) - Worker 10개는 Launcher가 실행
    javac -encoding UTF-8 -d out src\*.java
    java -cp out Launcher --coord <서버 IP>:8000 --workers 10 --peer-ip 127.0.0.1 --peer-base 7000 --step-delay 100 --seed 11 --log-dir logs

  정합성 확인 (실행 종료 후, Windows)
    scp -i <키.pem> ubuntu@<서버 IP>:~/hw3/logs/Coordinator.txt logs\
    java -cp out LogChecker logs
  LogChecker는 Coordinator.txt, Worker1~10.txt, Launcher.txt가 logs 폴더에 모두 있어야 함.

  인자
    Coordinator  --host 바인딩 IP, --port 포트, --log 로그 파일
    Launcher     --coord 서버 주소:포트, --workers 10, --peer-ip / --peer-base Worker Peer 주소(7000+i),
                 --step-delay ms, --seed 난수 seed, --log-dir 로그 폴더
                 --manual (콘솔에 "crash 4", "restart 4"로 수동 장애), --no-chaos (장애 끄기) 는 개발용
  나머지(Task 1000개, Dispatcher 6개, HB 1초, SUSPECT 3초, DEAD 5초, Launcher 시간 규칙)는 코드에 고정.
  최종 실행 약 2분.


5. 장애 감지 (Heartbeat / Failure Detector)
  Worker는 RUNNING일 때만 1초마다 HB 전송, Listener가 바로 lastHB 갱신.
  Failure Detector가 1초마다 검사: 3초 이상 없으면 SUSPECT(로그만), 5초 이상 없으면 DEAD.


6. Checkpoint / Takeover
  Checkpoint: step 1~9마다 Buddy(i mod 10 + 1)에게 CKPT 전송. Buddy는 Task마다 최신 것만 저장.
  Takeover:   DEAD Worker의 Task 전부(인계받은 것 포함)를 다음 ALIVE에게. epoch + 1 후 TAKEOVER 전송.
              받은 Worker는 CKPT가 있으면 다음 step부터, 없으면 step 1부터 계산.


7. Epoch / Commit (이중 Commit 방지)
  epoch는 Coordinator만 변경 (ASSIGN 때 1, TAKEOVER마다 +1).
  RESULT는 DEAD / 이미 DONE / epoch·owner 불일치면 REJECT_STALE, 아니면 Commit 후 ACK.
  확인과 Commit은 Task Lock 안에서 한 번에 처리 (Lock 밖에서 하면 commit_count = 2가 될 수 있음).
  Lock 순서는 항상 Worker Table -> Task(번호 오름차순), send는 Lock 해제 후. 대기는 전부 Condition Variable.


8. Rejoin / Worker의 crash 처리
  crash:  Queue, 진행 중 Task, Checkpoint Store를 지우고 침묵 (TCP 연결 유지, BYE·RESTART만 처리).
          보내기 직전에 crashGen을 확인해서 crash 전 Task의 메시지가 새지 않게 함.
  Rejoin: RESTART -> REJOIN. DEAD면 WELCOME, 아직 아니면 RETRY_LATER(2초 뒤 재시도).
          WELCOME 후 HB 재개, REQ로 새 Task를 받음.


9. 정합성 확인
  정합성 확인 (LogChecker, 결과는 logs/check_result.txt)
    1) 1000 Task DONE, commit_count 합 = 1000 (이중 Commit 0건)
    2) 합계 = 5,761,455
    3) Launcher CRASH 수 = Coordinator DEAD 수 = REJOIN 수 (실제 시각으로 대조)
    4) 어느 순간에도 DEAD 수 <= 2
    5) crash마다 재합류 후 새 Task 수행
    6) Coordinator COMMIT 수 = 1000
  하나라도 실패하면 FAIL, 종료 코드 1.
  CKPT 절약률 = crash별 Σ(재개 step - 1) / (인계 Task 수 x 10), Worker 로그의 TAKEOVER 재개 step으로 계산.

  실측 결과 (원격 구성, seed 11)
    실행 시간 / 처리량    123.3 sec / 8.11 task/s
    합계                  5,761,455 (PASS)
    이중 Commit           0 건
    CRASH / DEAD / REJOIN 7 / 7 / 7
    TAKEOVER              8 건
    REJECT_STALE          0 건
    최대 동시 DEAD        2
    CKPT 절약             22 / 80 step

  crash별 지표 (시각은 START 기준 초)
    #  worker  lastHB  DEAD    감지     인계               복구   재합류   첫 Task(지연)   재합류 후 Commit  CKPT 절약
    1  W2      17.20   23.02   5.82s   159 -> W3          1.9s   28.75    246 (0.01s)     16                1/10
    2  W4      26.31   32.03   5.72s   226 -> W5          6.7s   37.61    311 (0.01s)     74                3/10
    3  W5      31.36   37.03   5.67s   226,265 -> W6      1.9s   44.32    361 (0.01s)     69                8/20
    4  W2      45.95   51.04   5.09s   386 -> W3          1.2s   61.49    494 (0.01s)     54                0/10
    5  W10     52.59   58.04   5.45s   429 -> W1          0.7s   67.79    537 (0.01s)     49                7/10
    6  W6      61.70   67.04   5.34s   496 -> W7          1.9s   74.43    589 (0.01s)     43                0/10
    7  W7      72.83   78.04   5.22s   573 -> W8          1.0s   86.77    686 (0.01s)     32                3/10
  #2·#3: 이웃 W4, W5 동시 crash. 226이 W4 -> W5 -> W6으로 두 번 인계(e3), CKPT가 W5와 함께 사라져 step 1부터.


10. 직접 정한 것들
  - RING / START: Main Thread가 전송.
  - REQ 시점: ACK를 기다리지 않고 RESULT 직후 (명세 의사코드 방식).
  - 인계 Task 순서: 진행 중 Task 먼저, 인계 Task는 그다음.
  - ALIVE가 없을 때: 첫 SUSPECT에게 인계 (명세와 QnA가 달라서 명세를 따름).
  - CKPT 절약률: Worker가 TAKEOVER를 받을 때 남긴 재개 step 기준, crash 중 버린 TAKEOVER는 0.
  - 송신 중 CRASH: 보내던 메시지가 끝난 뒤 CRASH 확정. 받은 메시지도 받은 시점의 crashGen으로 재확인.


11. 기타 / 한계
  - EC2와 로컬 PC 시계가 약 0.1초 어긋나서, Worker 수신 시각이 Coordinator 송신 시각보다 앞서 찍힌 곳이 있음.
  - 동시 crash 최대 2개를 가정. Worker가 전부 죽으면 넘길 곳이 없어 끝나지 않음.
    Task를 PENDING으로 되돌려 재합류한 Worker가 가져가게 하는 방법을 생각했지만 시간 문제로 구현 못 함.
  - SUSPECT(HB 3초 없음)일 때는 로그만 남겨서 인계까지 항상 5~6초 걸림. SUSPECT 시점에 다음 Worker가 미리
    준비하면 빨라지지만, 오판일 때 중복 계산 문제를 해결하지 못해 구현 못 함.
