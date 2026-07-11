package main

import (
	"context"
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

func main() {
	// 1. flag 解析
	portFlag := flag.Int("port", 0, "监听端口（0 表示使用配置文件中的值）")
	configFlag := flag.String("config", "sawtooth.yaml", "配置文件路径")
	flag.Parse()

	// 2. 日志初始化
	logger := slog.New(proxy.NewLogHandler(os.Stdout, slog.LevelInfo))
	slog.SetDefault(logger)

	// 3. Config 加载
	cfg, err := proxy.LoadConfig(*configFlag)
	if err != nil {
		slog.Error("加载配置失败", "path", *configFlag, "error", err)
		os.Exit(1)
	}

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
		os.Exit(1)
	}
	srv.TokenCounter = tc
	slog.Info("token 计数器已初始化", "encoding", "cl100k_base")

	// Phase 3: SQLite store 初始化 (D-13, D-14)
	dbPath := filepath.Join(cfg.Debug.DataDir, "sawtooth.db")
	store, err := proxy.NewSQLiteStore(dbPath)
	if err != nil {
		slog.Error("初始化 SQLite 存储失败", "path", dbPath, "error", err)
		os.Exit(1)
	}
	srv.Store = store
	slog.Info("SQLite 存储已初始化", "path", dbPath)

	// Phase B: DecayTracker 初始化（per-message decay tracking）
	srv.DecayTracker = proxy.NewDecayTracker()
	srv.DecayTracker.SetPersistFunc(func(key, value string) {
		_ = store.PersistState(key, value) // best-effort
	})
	srv.DecayTracker.SetLoadFunc(store.LoadState)
	slog.Info("decay tracker 已初始化（per-message + SQLite 持久化）")

	// Phase 4: FrozenStubs + SawtoothTrigger 初始化 (D-03, D-12)
	frozenStore := proxy.NewFrozenStubsWithTTL(
		proxy.SawtoothTTLForCacheTTL(cfg.Cache.CacheTTL), // Phase 8 D7: 从 cache_ttl 推导
	)
	frozenStore.SetPersistFunc(func(key, value string) {
		_ = store.PersistState(key, value) // best-effort，忽略错误
	})
	frozenStore.SetLoadFunc(store.LoadState)
	frozenStore.SetDeleteFunc(func(key string) {
		_ = store.DeleteState(key) // best-effort，内存标记仍防止当前进程重复加载
	})
	if cfg.Frozen.Enabled {
		srv.Frozen = frozenStore
	}
	slog.Info("FrozenStubs 已初始化",
		"ttl_minutes", cfg.Frozen.TTLMinutes,
		"enabled", cfg.Frozen.Enabled,
	)

	sawtoothTrigger := proxy.NewSawtoothTrigger(
		proxy.CacheGapForTTL(cfg.Cache.CacheTTL), // Phase 8 D7: 从 cache_ttl 推导（替代硬编码 330s）
		cfg.Stubify.TokenThreshold,               // tokenThreshold (D-03: 复用 stubify)
		cfg.Stubify.TokenThreshold/2,             // tokenMinimum (D-03: threshold/2)
	)
	sawtoothTrigger.SetPersistFunc(func(key, value string) {
		_ = store.PersistState(key, value) // best-effort，忽略错误
	})
	sawtoothTrigger.SetLoadFunc(store.LoadState)
	srv.Sawtooth = sawtoothTrigger
	slog.Info("SawtoothTrigger 已初始化",
		"token_threshold", cfg.Stubify.TokenThreshold,
		"token_minimum", cfg.Stubify.TokenThreshold/2,
		"pause_threshold", "330s",
	)

	// Phase 5: EagerStubMemory 初始化 (EAGER-02)
	eagerStubMem := proxy.NewEagerStubMemory()
	eagerStubMem.SetPersistFunc(func(key, value string) {
		_ = store.PersistState(key, value) // best-effort
	})
	eagerStubMem.SetLoadFunc(store.LoadState)
	srv.EagerStub = eagerStubMem
	slog.Info("EagerStubMemory 已初始化")

	// Phase 4: FrozenStubs 定时 eviction goroutine（每 5 分钟，D-12）
	// evictCtx/evictCancel 在 if 外部声明——信号处理器需要 evictCancel。
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

	// 6. 信号处理（Ctrl+C 优雅退出）
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		slog.Info("正在关闭...")
		evictCancel() // Phase 4: 先停止 eviction goroutine
		if srv.Store != nil {
			if err := srv.Store.Close(); err != nil {
				slog.Error("关闭 SQLite 存储失败", "error", err)
			}
		}
		os.Exit(0)
	}()

	// 7. 启动监听
	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	slog.Info("启动代理", "addr", addr, "target", cfg.Proxy.Target)

	if err := http.ListenAndServe(addr, mux); err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "address already in use") ||
			strings.Contains(errStr, "bind") ||
			strings.Contains(errStr, "Access") ||
			strings.Contains(errStr, "permission denied") {
			slog.Error("端口已被占用", "addr", addr, "error", err)
		} else {
			slog.Error("服务启动失败", "addr", addr, "error", err)
		}
		os.Exit(1)
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
