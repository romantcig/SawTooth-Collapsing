package proxy

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
)

// ── Plan 12.1-05 Task 2：DecayTracker 的 error-aware 加载与 stateKey 持久化隔离 ──
//
// stateLoaderFunc、stateLoadErrorCases、reentrantStateSubmitter 与 runWithinTimeout
// 由 frozen_test.go 提供，四个状态组件共用同一套失败注入语义。

// decayStateValue 构造一条持久化记录，供前缀过滤与污染测试注入。
func decayStateValue(t *testing.T, stubbedAt map[string]int, intensity map[string]float64, filePaths map[string]string) string {
	t.Helper()
	data, err := json.Marshal(decayPersisted{StubbedAt: stubbedAt, Intensity: intensity, FilePaths: filePaths})
	if err != nil {
		t.Fatalf("构造 decay 记录失败: %v", err)
	}
	return string(data)
}

// assertDecayRecordScoped 断言一条持久化记录的所有 key 都属于给定 stateKey。
func assertDecayRecordScoped(t *testing.T, raw, stateKey string) decayPersisted {
	t.Helper()
	if raw == "" {
		t.Fatalf("%s 未产生持久化记录", stateKey)
	}
	var dp decayPersisted
	if err := json.Unmarshal([]byte(raw), &dp); err != nil {
		t.Fatalf("解析 decay 记录失败: %v", err)
	}
	prefix := stateKey + ":msg_"
	for key := range dp.StubbedAt {
		if !strings.HasPrefix(key, prefix) {
			t.Fatalf("%s 的快照混入了外部 key: %q", stateKey, key)
		}
	}
	for key := range dp.Intensity {
		if !strings.HasPrefix(key, prefix) {
			t.Fatalf("%s 的强度快照混入了外部 key: %q", stateKey, key)
		}
	}
	for key := range dp.FilePaths {
		if !strings.HasPrefix(key, prefix) {
			t.Fatalf("%s 的路径快照混入了外部 key: %q", stateKey, key)
		}
	}
	return dp
}

func TestDecayStateLoaderMissingVsError(t *testing.T) {
	const stateKey = "decay-loader-truth"

	t.Run("missing", func(t *testing.T) {
		tracker := NewDecayTracker()
		calls := 0
		tracker.SetStateLoader(stateLoaderFunc(func(string) StateLoadResult {
			calls++
			return StateLoadResult{}
		}))
		if failure := tracker.LoadFromDB(stateKey); failure.Failed {
			t.Fatalf("missing 应是正常冷启动: %+v", failure)
		}
		tracker.mu.RLock()
		loaded, found := tracker.loadedFromDB[stateKey], tracker.stateFoundDB[stateKey]
		tracker.mu.RUnlock()
		if !loaded || found {
			t.Fatalf("missing 后 loaded=%v found=%v, want true/false", loaded, found)
		}
		tracker.LoadFromDB(stateKey)
		if calls != 1 {
			t.Fatalf("missing 是终态，却查询了 %d 次", calls)
		}
	})

	for name, failureResult := range stateLoadErrorCases() {
		t.Run(name, func(t *testing.T) {
			tracker := NewDecayTracker()
			calls := 0
			tracker.SetStateLoader(stateLoaderFunc(func(string) StateLoadResult {
				calls++
				return failureResult
			}))
			failure := tracker.LoadFromDB(stateKey)
			if !failure.Failed || failure.Class != failureResult.FailureClass {
				t.Fatalf("failure=%+v, want class %q", failure, failureResult.FailureClass)
			}
			tracker.mu.RLock()
			loaded, found := tracker.loadedFromDB[stateKey], tracker.stateFoundDB[stateKey]
			tracker.mu.RUnlock()
			if loaded || found {
				t.Fatalf("读取失败被当成冷启动 missing: loaded=%v found=%v", loaded, found)
			}
			tracker.LoadFromDB(stateKey)
			if calls != 2 {
				t.Fatalf("读取失败后不可重试，查询次数=%d", calls)
			}
		})
	}
}

func TestDecayLoadFailureFailClosedAndRetry(t *testing.T) {
	const stateKey = "decay-retry"
	value := decayStateValue(t,
		map[string]int{stateKey + ":msg_0": 7},
		map[string]float64{stateKey + ":msg_0": 0.5},
		map[string]string{stateKey + ":msg_0": "/tmp/a.go"})

	calls := 0
	tracker := NewDecayTracker()
	tracker.SetStateLoader(stateLoaderFunc(func(string) StateLoadResult {
		calls++
		if calls == 1 {
			return StateLoadResult{Err: ErrStateLoadBusy, FailureClass: StateLoadFailureSQLiteBusy}
		}
		return StateLoadResult{Value: value, Found: true}
	}))

	failure := tracker.LoadFromDB(stateKey)
	if !failure.Failed || failure.Class != StateLoadFailureSQLiteBusy {
		t.Fatalf("busy 未上报为读取失败: %+v", failure)
	}
	tracker.mu.RLock()
	applied := len(tracker.stubbedAt)
	tracker.mu.RUnlock()
	if applied != 0 {
		t.Fatalf("读取失败仍应用了 %d 条旧状态", applied)
	}

	if failure := tracker.LoadFromDB(stateKey); failure.Failed {
		t.Fatalf("后续读取仍失败: %+v", failure)
	}
	tracker.mu.RLock()
	restored := tracker.stubbedAt[stateKey+":msg_0"]
	tracker.mu.RUnlock()
	if restored != 7 {
		t.Fatalf("后续成功读取未恢复状态: stubbedAt=%d", restored)
	}
}

func TestDecayMigrationTargetErrorStopsLegacy(t *testing.T) {
	const sessionID = "decay-migrate"
	stateKey := historyEpochStateKey(sessionID, 1)
	legacyValue := decayStateValue(t,
		map[string]int{sessionID + ":msg_0": 3},
		map[string]float64{sessionID + ":msg_0": 0.25},
		map[string]string{sessionID + ":msg_0": "/tmp/legacy.go"})

	for name, failureResult := range stateLoadErrorCases() {
		t.Run(name, func(t *testing.T) {
			var queried []string
			tracker := NewDecayTracker()
			tracker.SetStateLoader(stateLoaderFunc(func(key string) StateLoadResult {
				queried = append(queried, key)
				if key == "decay:"+stateKey {
					return failureResult
				}
				return StateLoadResult{Value: legacyValue, Found: true}
			}))
			failure := tracker.MigrateLegacyState(sessionID, stateKey)
			if !failure.Failed || failure.Class != failureResult.FailureClass {
				t.Fatalf("target 读取失败未上报: %+v", failure)
			}
			if len(queried) != 1 || queried[0] != "decay:"+stateKey {
				t.Fatalf("target 失败后仍读取 legacy: %v", queried)
			}
			tracker.mu.RLock()
			migrated := tracker.hasSessionStateLocked(stateKey)
			tracker.mu.RUnlock()
			if migrated {
				t.Fatal("target 读取失败后仍复用 legacy 状态")
			}
		})
	}

	var queried []string
	persisted := map[string]string{}
	tracker := NewDecayTracker()
	tracker.SetStateLoader(stateLoaderFunc(func(key string) StateLoadResult {
		queried = append(queried, key)
		if key == "decay:"+stateKey {
			return StateLoadResult{}
		}
		return StateLoadResult{Value: legacyValue, Found: true}
	}))
	tracker.SetPersistFunc(func(key, value string) { persisted[key] = value })
	if failure := tracker.MigrateLegacyState(sessionID, stateKey); failure.Failed {
		t.Fatalf("missing 不应被当作失败: %+v", failure)
	}
	tracker.mu.RLock()
	migrated := tracker.stubbedAt[stateKey+":msg_0"]
	tracker.mu.RUnlock()
	if migrated != 3 {
		t.Fatalf("epoch 1 legacy migration 未执行: stubbedAt=%d", migrated)
	}
	if len(queried) != 2 {
		t.Fatalf("migration 查询序列=%v, want target 后再读 legacy", queried)
	}
	assertDecayRecordScoped(t, persisted["decay:"+stateKey], stateKey)
}

func TestDecayResultBearingPersistence(t *testing.T) {
	const stateKey = "decay-result-bearing"
	backend := newPersistenceFakeBackend()
	writer, _, _ := newPersistenceWriterTest(t, backend)
	defer func() { _ = writer.CloseAndDrain() }()

	tracker := NewDecayTracker()
	// 提交必须发生在组件 mutex 之外：submitter 回读时获取写锁，锁内提交会自锁死。
	tracker.SetStateSubmitter(reentrantStateSubmitter{
		inner: writer,
		probe: func() { tracker.SetCurrentAlias("decay-probe-session", stateKey) },
	})

	// 1. 磁盘阻塞时内存先行，当前请求不被 SQLite 拖住。
	tracker.MarkStubbed(stateKey, 0, 5, 0.3)
	started := backend.blockKey("decay:" + stateKey)
	blockedProbe := newPersistenceProbe(t, 1)
	var blockedReceipt PersistenceReceipt
	runWithinTimeout(t, "Decay 阻塞写入", func() {
		blockedReceipt = tracker.PersistWithResult(stateKey, blockedProbe.completion)
	})
	if !blockedReceipt.Accepted {
		t.Fatalf("阻塞写入未被接受: %+v", blockedReceipt)
	}
	<-started
	if got := blockedProbe.diskState(); got == persistenceStateSaved {
		t.Fatalf("SQLite 尚未返回就记为 saved: %q", got)
	}
	backend.releaseAll()
	waitForPersistence(t, func() bool { return blockedProbe.diskState() == persistenceStateSaved })

	// 2. SQLite 失败如实进入 request closure，且不回滚内存状态。
	backend.failKey("decay:"+stateKey, true)
	failedProbe := newPersistenceProbe(t, 2)
	runWithinTimeout(t, "Decay 失败写入", func() {
		tracker.MarkStubbed(stateKey, 1, 6, 0.4)
		tracker.PersistWithResult(stateKey, failedProbe.completion)
	})
	waitForPersistence(t, func() bool { return failedProbe.diskState() == persistenceStateFailed })
	if got := failedProbe.failureClass(); got != persistenceFailureSQLite {
		t.Fatalf("失败类别=%q, want sqlite", got)
	}
	tracker.mu.RLock()
	_, kept := tracker.stubbedAt[stateKey+":msg_1"]
	tracker.mu.RUnlock()
	if !kept {
		t.Fatal("磁盘失败不应回滚内存状态")
	}

	// 3. ClearSession 同样 result-bearing，且提交在锁外。
	backend.failKey("decay:"+stateKey, false)
	clearProbe := newPersistenceProbe(t, 3)
	runWithinTimeout(t, "Decay 清理写入", func() {
		tracker.ClearSessionWithResult(stateKey, clearProbe.completion)
	})
	waitForPersistence(t, func() bool { return clearProbe.diskState() == persistenceStateSaved })
	tracker.mu.RLock()
	remaining := len(tracker.stubbedAt)
	tracker.mu.RUnlock()
	if remaining != 0 {
		t.Fatalf("ClearSession 后仍残留 %d 条记录", remaining)
	}

	// 4. 兼容 void callback 不得产生 saved 事实。
	legacy := NewDecayTracker()
	legacy.SetPersistFunc(func(string, string) {})
	legacy.MarkStubbed(stateKey, 0, 5, 0.3)
	legacyProbe := newPersistenceProbe(t, 4)
	receipt := legacy.PersistWithResult(stateKey, legacyProbe.completion)
	if receipt.Result.State == persistenceStateSaved || legacyProbe.diskState() == persistenceStateSaved {
		t.Fatalf("void callback 冒充了 saved: receipt=%+v disk=%q", receipt, legacyProbe.diskState())
	}
}

func TestDecaySnapshotFiltersStateKeyPrefix(t *testing.T) {
	keyA := historyEpochStateKey("session-a", 1)
	keyB := historyEpochStateKey("session-b", 2)

	var mu sync.Mutex
	persisted := map[string]string{}
	tracker := NewDecayTracker()
	tracker.SetPersistFunc(func(key, value string) {
		mu.Lock()
		persisted[key] = value
		mu.Unlock()
	})

	tracker.MarkStubbed(keyA, 0, 5, 0.4)
	tracker.SetFilePath(keyA, 0, "/a.go")
	tracker.MarkStubbed(keyB, 1, 9, 0.6)
	tracker.SetFilePath(keyB, 1, "/b.go")

	tracker.Persist(keyA)
	tracker.Persist(keyB)

	mu.Lock()
	rawA, rawB := persisted["decay:"+keyA], persisted["decay:"+keyB]
	mu.Unlock()
	dpA := assertDecayRecordScoped(t, rawA, keyA)
	dpB := assertDecayRecordScoped(t, rawB, keyB)
	if len(dpA.StubbedAt) != 1 || len(dpB.StubbedAt) != 1 {
		t.Fatalf("快照条数 A=%d B=%d, want 各 1 条", len(dpA.StubbedAt), len(dpB.StubbedAt))
	}

	// PersistUnlocked 必须复用同一过滤快照，而不是序列化三张全局 map。
	tracker.mu.Lock()
	unlockedKey, snapshot := tracker.PersistUnlocked(keyA)
	tracker.mu.Unlock()
	if unlockedKey != keyA {
		t.Fatalf("PersistUnlocked 解析出的 stateKey=%q, want %q", unlockedKey, keyA)
	}
	if len(snapshot.StubbedAt) != 1 {
		t.Fatalf("PersistUnlocked 快照条数=%d, want 1", len(snapshot.StubbedAt))
	}
	if _, leaked := snapshot.StubbedAt[keyB+":msg_1"]; leaked {
		t.Fatal("PersistUnlocked 快照泄漏了另一 epoch 的记录")
	}
}

func TestDecayLoadFiltersPollutedRecord(t *testing.T) {
	keyA := historyEpochStateKey("session-a", 1)
	keyB := historyEpochStateKey("session-b", 1)
	polluted := decayStateValue(t,
		map[string]int{keyA + ":msg_0": 4, keyB + ":msg_1": 9, "msg_2": 11},
		map[string]float64{keyA + ":msg_0": 0.2, keyB + ":msg_1": 0.9, "msg_2": 0.7},
		map[string]string{keyA + ":msg_0": "/a.go", keyB + ":msg_1": "/b.go", "msg_2": "/bare.go"})

	tracker := NewDecayTracker()
	tracker.SetStateLoader(stateLoaderFunc(func(string) StateLoadResult {
		return StateLoadResult{Value: polluted, Found: true}
	}))
	if failure := tracker.LoadFromDB(keyA); failure.Failed {
		t.Fatalf("污染记录不应被当作读取失败: %+v", failure)
	}

	tracker.mu.RLock()
	total := len(tracker.stubbedAt)
	_, hasOwn := tracker.stubbedAt[keyA+":msg_0"]
	_, hasForeign := tracker.stubbedAt[keyB+":msg_1"]
	_, hasBare := tracker.stubbedAt["msg_2"]
	pollution := tracker.loadPollution[keyA]
	tracker.mu.RUnlock()

	if !hasOwn || hasForeign || hasBare || total != 1 {
		t.Fatalf("污染记录未被过滤: total=%d own=%v foreign=%v bare=%v", total, hasOwn, hasForeign, hasBare)
	}
	// 三张 map 各贡献一条外部前缀与一条裸 key。
	if pollution.ForeignPrefix != 3 || pollution.BareMessage != 3 {
		t.Fatalf("污染计数=%+v, want foreign=3 bare=3", pollution)
	}
}

func TestDecayRejectsBareMessageKeys(t *testing.T) {
	keyA := historyEpochStateKey("bare-session", 1)
	bare := decayStateValue(t,
		map[string]int{"msg_0": 1},
		map[string]float64{"msg_0": 0.1},
		map[string]string{"msg_0": "/bare.go"})

	persisted := map[string]string{}
	tracker := NewDecayTracker()
	tracker.SetStateLoader(stateLoaderFunc(func(string) StateLoadResult {
		return StateLoadResult{Value: bare, Found: true}
	}))
	tracker.SetPersistFunc(func(key, value string) { persisted[key] = value })

	if failure := tracker.LoadFromDB(keyA); failure.Failed {
		t.Fatalf("裸 key 记录不应被当作读取失败: %+v", failure)
	}
	tracker.mu.RLock()
	imported := len(tracker.stubbedAt)
	pollution := tracker.loadPollution[keyA]
	tracker.mu.RUnlock()
	if imported != 0 {
		t.Fatalf("裸 msg_N 被自动归属，导入 %d 条", imported)
	}
	if pollution.BareMessage != 3 {
		t.Fatalf("裸 key 计数=%+v, want 3", pollution)
	}

	// 内存中若已有历史遗留的裸 key，也不得被写回任何 stateKey 的快照。
	tracker.mu.Lock()
	tracker.stubbedAt["msg_9"] = 2
	tracker.mu.Unlock()
	tracker.MarkStubbed(keyA, 0, 5, 0.3)
	tracker.Persist(keyA)
	dp := assertDecayRecordScoped(t, persisted["decay:"+keyA], keyA)
	if _, leaked := dp.StubbedAt["msg_9"]; leaked {
		t.Fatal("快照写回了无法归属的裸 key")
	}
}

func TestDecayClearAndMigrationUseFilteredSnapshot(t *testing.T) {
	keyA := historyEpochStateKey("clear-a", 1)
	keyB := historyEpochStateKey("clear-b", 1)

	t.Run("clear", func(t *testing.T) {
		var mu sync.Mutex
		persisted := map[string]string{}
		tracker := NewDecayTracker()
		tracker.SetPersistFunc(func(key, value string) {
			mu.Lock()
			persisted[key] = value
			mu.Unlock()
		})
		tracker.MarkStubbed(keyA, 0, 5, 0.4)
		tracker.MarkStubbed(keyB, 0, 5, 0.4)
		tracker.ClearSession(keyA)

		tracker.mu.RLock()
		_, clearedA := tracker.stubbedAt[keyA+":msg_0"]
		_, keptB := tracker.stubbedAt[keyB+":msg_0"]
		tracker.mu.RUnlock()
		if clearedA || !keptB {
			t.Fatalf("ClearSession 越界: A 残留=%v B 被清=%v", clearedA, !keptB)
		}
		mu.Lock()
		raw := persisted["decay:"+keyA]
		mu.Unlock()
		dp := assertDecayRecordScoped(t, raw, keyA)
		if len(dp.StubbedAt) != 0 {
			t.Fatalf("清空后的快照仍含 %d 条记录", len(dp.StubbedAt))
		}
	})

	t.Run("migration", func(t *testing.T) {
		const sessionID = "migrate-filtered"
		stateKey := historyEpochStateKey(sessionID, 1)
		legacyValue := decayStateValue(t,
			map[string]int{sessionID + ":msg_0": 3, keyB + ":msg_7": 8},
			map[string]float64{sessionID + ":msg_0": 0.25},
			map[string]string{sessionID + ":msg_0": "/legacy.go"})

		persisted := map[string]string{}
		tracker := NewDecayTracker()
		tracker.SetStateLoader(stateLoaderFunc(func(key string) StateLoadResult {
			if key == "decay:"+stateKey {
				return StateLoadResult{}
			}
			return StateLoadResult{Value: legacyValue, Found: true}
		}))
		tracker.SetPersistFunc(func(key, value string) { persisted[key] = value })
		if failure := tracker.MigrateLegacyState(sessionID, stateKey); failure.Failed {
			t.Fatalf("migration 失败: %+v", failure)
		}
		dp := assertDecayRecordScoped(t, persisted["decay:"+stateKey], stateKey)
		if len(dp.StubbedAt) != 1 {
			t.Fatalf("migration 快照条数=%d, want 1", len(dp.StubbedAt))
		}
	})

	t.Run("concurrent", func(t *testing.T) {
		var mu sync.Mutex
		persisted := map[string][]string{}
		tracker := NewDecayTracker()
		tracker.SetPersistFunc(func(key, value string) {
			mu.Lock()
			persisted[key] = append(persisted[key], value)
			mu.Unlock()
		})

		start := make(chan struct{})
		var wg sync.WaitGroup
		for _, key := range []string{keyA, keyB} {
			wg.Add(1)
			go func(stateKey string) {
				defer wg.Done()
				<-start
				for index := 0; index < 20; index++ {
					tracker.MarkStubbed(stateKey, index, index, 0.5)
					tracker.SetFilePath(stateKey, index, "/x.go")
					tracker.Persist(stateKey)
				}
			}(key)
		}
		close(start)
		wg.Wait()

		mu.Lock()
		defer mu.Unlock()
		for _, stateKey := range []string{keyA, keyB} {
			values := persisted["decay:"+stateKey]
			if len(values) == 0 {
				t.Fatalf("%s 未产生任何持久化记录", stateKey)
			}
			for _, raw := range values {
				assertDecayRecordScoped(t, raw, stateKey)
			}
		}
	})
}
