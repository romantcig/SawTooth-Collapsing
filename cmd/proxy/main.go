package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/romantcig/sawtooth-collapsing/internal/proxy"
)

// lifecycleStage 是关闭流程的受限阶段名。它只用于顺序审计，不含任何请求事实。
type lifecycleStage string

const (
	stageHTTPStopped        lifecycleStage = "http_stopped"
	stagePersistenceDrained lifecycleStage = "persistence_drained"
	stageOutcomeDrained     lifecycleStage = "outcome_drained"
	stageStoreClosed        lifecycleStage = "store_closed"
)

// shutdownStep 是一个可独立失败、但不阻断后续安全阶段的关闭动作。
type shutdownStep struct {
	stage lifecycleStage
	run   func() error
}

// productionShutdownSteps 固化唯一合法的关闭顺序：
// 先停止接收并等待 active handler 返回/seal，再排空普通 state 持久化，
// 让所有已接受 receipt 完成，再排空 outcome 投影，最后才关闭 SQLite。
func productionShutdownSteps(stopHTTP, drainPersistence, drainOutcome, closeStore func() error) []shutdownStep {
	return []shutdownStep{
		{stage: stageHTTPStopped, run: stopHTTP},
		{stage: stagePersistenceDrained, run: drainPersistence},
		{stage: stageOutcomeDrained, run: drainOutcome},
		{stage: stageStoreClosed, run: closeStore},
	}
}

// runShutdown 按给定顺序执行全部阶段。前置阶段失败只被记录，后续仍可安全
// 执行的阶段继续执行——提前退出会留下未完成的 receipt 或未关闭的数据库。
func runShutdown(steps []shutdownStep, record func(lifecycleStage)) []error {
	var errs []error
	for _, step := range steps {
		var err error
		if step.run != nil {
			err = step.run()
		}
		if record != nil {
			record(step.stage)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", step.stage, err))
		}
	}
	return errs
}

func main() {
	if code := run(); code != 0 {
		os.Exit(code)
	}
}

func run() int {
	// 1. flag 解析
	portFlag := flag.Int("port", 0, "监听端口（0 表示使用配置文件中的值）")
	configFlag := flag.String("config", "sawtooth.yaml", "配置文件路径")
	verboseFlag := flag.Bool("v", false, "终端显示 Debug 级日志（文件日志级别不受影响）")
	flag.Parse()

	// 2. 日志初始化：-v 只调终端 handler 级别 Info → Debug，文件侧固定
	// LevelDebug 全量收录。
	terminalLevel := slog.LevelInfo
	if *verboseFlag {
		terminalLevel = slog.LevelDebug
	}
	terminalHandler := proxy.NewLogHandler(os.Stdout, terminalLevel)
	terminalLogger := slog.New(terminalHandler)
	slog.SetDefault(terminalLogger)

	// 3. Config 加载
	cfg, err := proxy.LoadConfig(*configFlag)
	if err != nil {
		slog.Error("加载配置失败", "path", *configFlag, "error", err)
		return 1
	}

	// 配置成功后才安装文件日志；配置读取失败保持仅终端可见。
	fileHandler := proxy.NewSessionLogHandler(cfg.Debug.DataDir, slog.LevelDebug, func(err error) {
		terminalLogger.Error("文件日志写入失败", "error", err)
	})
	slog.SetDefault(slog.New(proxy.NewCombinedLogHandler(terminalHandler, fileHandler)))
	logFullBodyDebugNotice(cfg)

	// -port 标志覆盖配置文件中的端口值
	if *portFlag != 0 {
		cfg.Server.Port = *portFlag
	}

	// 4. Server 创建
	srv := proxy.NewServer(cfg)

	// Phase 2: Token 计数器和衰减管理器初始化 (D-01, D-11)
	tc, err := proxy.NewTokenCounter()
	if err != nil {
		slog.Error("初始化 token 计数器失败", "error", err)
		return 1
	}
	srv.TokenCounter = tc
	slog.Info("token 计数器已初始化", "encoding", "cl100k_base")

	// Phase 3: SQLite store 初始化 (D-13, D-14)
	dbPath := filepath.Join(cfg.Debug.DataDir, "sawtooth.db")
	store, err := proxy.NewSQLiteStore(dbPath)
	if err != nil {
		slog.Error("初始化 SQLite 存储失败", "path", dbPath, "error", err)
		return 1
	}
	srv.Store = store
	slog.Info("SQLite 存储已初始化", "path", dbPath)

	// Phase 12.1: 一个共享 HealthTracker + 一个 terminal-only reporter。
	// reporter 直接绑定终端 handler，因此正在失败的文件 sink 不可能被回写。
	healthTracker := proxy.NewHealthTracker()
	healthReporter := proxy.NewTerminalHealthReporter(terminalHandler)
	healthTracker.SetReporter(healthReporter)

	// 唯一的进程级普通 state writer：Archive 与 history transition 在类型层面
	// 就进不了它的队列。
	persistenceWriter, err := proxy.NewPersistenceWriterChecked(store, healthTracker, healthReporter)
	if err != nil {
		slog.Error("初始化状态持久化 writer 失败", "error", err)
		return 1
	}

	// 唯一的进程级 outcome dispatcher：terminal 与 session 两条独立 lane，
	// 与 session outcome writer 共享同一份缺口证据。
	outcomeGaps := proxy.NewOutcomeGapAccumulator()
	sessionOutcomeWriter := proxy.NewSessionOutcomeWriter(fileHandler, outcomeGaps, healthTracker)
	outcomeDispatcher, err := proxy.NewOutcomeDispatcherChecked(proxy.OutcomeDispatcherOptions{
		TerminalProjector: terminalHandler,
		SessionProjector:  sessionOutcomeWriter,
		HealthTracker:     healthTracker,
		Reporter:          healthReporter,
		GapAccumulator:    outcomeGaps,
	})
	if err != nil {
		slog.Error("初始化结果投影 dispatcher 失败", "error", err)
		return 1
	}
	srv.SetOutcomeDispatcher(outcomeDispatcher)
	// 同步 Archive commit 与普通 state 共用同一 tracker/reporter，但不共用队列。
	srv.SetArchiveHealthObserver(persistenceWriter)

	// Phase 11: history epoch 只持久化数字、受限原因和固定长度 hash；
	// 状态键由 session + 本地单调 epoch 构成，不保存历史正文。
	// 它刻意不接普通 writer：epoch 与 Archive 可见性必须原子提交。
	if srv.HistoryEpoch == nil {
		srv.HistoryEpoch = proxy.NewHistoryEpochManager()
	}
	srv.HistoryEpoch.SetPersistFunc(func(key, value string) {
		observeStateWrite(healthTracker, "history epoch", store.PersistState(key, value))
	})
	srv.HistoryEpoch.SetStateLoader(store)
	srv.HistoryEpoch.SetTransitionFunc(store.CommitHistoryTransition)
	slog.Info("history epoch 已初始化")

	// Phase B: DecayTracker 初始化（per-message decay tracking）
	srv.DecayTracker = proxy.NewDecayTracker()
	srv.DecayTracker.SetStateSubmitter(persistenceWriter)
	srv.DecayTracker.SetStateLoader(store)
	slog.Info("decay tracker 已初始化（per-message + SQLite 持久化）")

	// Phase 4: FrozenStubs + SawtoothTrigger 初始化 (D-03, D-12)
	frozenStore := proxy.NewFrozenStubsWithTTL(
		proxy.SawtoothTTLForCacheTTL(cfg.Cache.CacheTTL), // Phase 8 D7: 从 cache_ttl 推导
	)
	frozenStore.SetStateSubmitter(persistenceWriter)
	frozenStore.SetStateLoader(store)
	if cfg.Frozen.Enabled {
		srv.Frozen = frozenStore
	}

	sawtoothTrigger := newSawtoothTrigger(cfg)
	sawtoothTrigger.SetStateSubmitter(persistenceWriter)
	sawtoothTrigger.SetStateLoader(store)
	srv.Sawtooth = sawtoothTrigger
	logSawtoothStartup(cfg.Frozen.Enabled)

	// Phase 4: FrozenStubs 定时 eviction goroutine（每 5 分钟，D-12）
	evictCtx, evictCancel := context.WithCancel(context.Background())
	defer evictCancel()
	if srv.Frozen != nil {
		go func() {
			ticker := time.NewTicker(5 * time.Minute)
			defer ticker.Stop()
			for {
				select {
				case <-evictCtx.Done():
					return
				case <-ticker.C:
					if evicted := srv.Frozen.Evict(); evicted > 0 {
						slog.Info("frozen stubs eviction", "evicted", evicted)
					}
				}
			}
		}()
	}

	// 5. 路由注册
	mux := http.NewServeMux()

	// POST /v1/messages → handleMessages（转发 + deflation + debug）
	mux.HandleFunc("POST /v1/messages", srv.HandleMessages)

	// 其他所有请求 → 反向代理到 Anthropic API
	reverseProxy := createReverseProxy(cfg.Proxy.Target)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		reverseProxy.ServeHTTP(w, r)
	})

	// 6. 显式 http.Server + signal context：关闭走正常控制流，不直接退出进程。
	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	httpServer := &http.Server{Addr: addr, Handler: mux}
	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stopSignals()

	serveErr := make(chan error, 1)
	go func() { serveErr <- httpServer.ListenAndServe() }()

	slog.Info("启动代理", "addr", addr, "target", cfg.Proxy.Target)

	exitCode := 0
	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logListenFailure(addr, err)
			exitCode = 1
		}
	case <-signalCtx.Done():
		slog.Info("正在关闭...")
	}
	evictCancel()

	// 7. 严格 drain 顺序：HTTP stop/wait → persistence → outcome → SQLite。
	shutdownErrs := runShutdown(productionShutdownSteps(
		func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			return httpServer.Shutdown(ctx)
		},
		persistenceWriter.CloseAndDrain,
		outcomeDispatcher.CloseAndDrain,
		store.Close,
	), nil)
	for _, err := range shutdownErrs {
		slog.Error("关闭阶段失败", "error", err)
		exitCode = 1
	}
	// 正常顺序下两类 closing rejection 必须为零；非零是 lifecycle 缺陷，
	// 不属于受控缺口，因此单独报告而不进入 gap accumulator。
	if rejected := persistenceWriter.LifecycleRejectionCount(); rejected != 0 {
		slog.Error("关闭期出现状态提交被拒", "count", rejected)
		exitCode = 1
	}
	if rejected := outcomeDispatcher.LifecycleRejectionCount(); rejected != 0 {
		slog.Error("关闭期出现结果投影被拒", "count", rejected)
		exitCode = 1
	}
	return exitCode
}

// observeStateWrite 让同步 state 写入的真实结果进入 sqlite_state health，
// 而不是被丢弃。它不吞掉 error，也不引入第二套 tracker。
func observeStateWrite(tracker *proxy.HealthTracker, what string, err error) {
	if err != nil {
		slog.Error("状态持久化失败", "component", what)
		tracker.ObserveFailure(proxy.HealthScopeSQLiteState, proxy.HealthFailureClassSQLiteState)
		return
	}
	tracker.ObserveSuccess(proxy.HealthScopeSQLiteState, proxy.HealthFailureClassSQLiteState)
}

func logListenFailure(addr string, err error) {
	errStr := err.Error()
	if strings.Contains(errStr, "address already in use") ||
		strings.Contains(errStr, "bind") ||
		strings.Contains(errStr, "Access") ||
		strings.Contains(errStr, "permission denied") {
		slog.Error("端口已被占用", "addr", addr, "error", err)
		return
	}
	slog.Error("服务启动失败", "addr", addr, "error", err)
}

func newSawtoothTrigger(cfg proxy.Config) *proxy.SawtoothTrigger {
	return proxy.NewSawtoothTrigger(
		proxy.CacheGapForTTL(cfg.Cache.CacheTTL), // 运行时等待只在 trigger 判定中生效
		cfg.Stubify.TokenThreshold,
		cfg.Stubify.TokenThreshold/2,
	)
}

func logSawtoothStartup(frozenEnabled bool) {
	slog.Info("FrozenStubs 已初始化", "enabled", frozenEnabled)
	slog.Info("SawtoothTrigger 已初始化")
}

func logFullBodyDebugNotice(cfg proxy.Config) {
	if cfg.Debug.Enabled && cfg.Debug.FullBody {
		slog.Info("完整正文 Debug 已开启")
	}
}

// createReverseProxy 创建指向 Anthropic API 的反向代理。
// 非 POST /v1/messages 的请求（包括 GET /v1/messages 及所有其他路径）
// 原样转发到上游，保留 Authorization header。
func createReverseProxy(targetURL string) *httputil.ReverseProxy {
	target, err := url.Parse(targetURL)
	if err != nil {
		slog.Error("无法解析目标 URL，回退到默认值", "target", targetURL, "error", err)
		target, _ = url.Parse("https://api.anthropic.com")
	}

	proxy := httputil.NewSingleHostReverseProxy(target)

	// 自定义 Director：保留原始 Authorization header
	originalDirector := proxy.Director
	proxy.Director = func(r *http.Request) {
		originalDirector(r)
		// 确保 Host header 正确设置
		r.Host = target.Host
	}

	return proxy
}
