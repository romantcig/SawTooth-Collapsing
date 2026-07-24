package proxy

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const debugRunIDBytes = 8

// debugArtifactStage 是 Debug 文件名的受限枚举。
// body 与 metadata 使用不同 stage，避免同一 request 目录互相覆盖。
type debugArtifactStage string

const (
	debugArtifactRawBody       debugArtifactStage = "raw"
	debugArtifactForwardedBody debugArtifactStage = "forwarded"
	debugArtifactResponseBody  debugArtifactStage = "response"
	debugArtifactRawMetadata   debugArtifactStage = "raw_meta"
	debugArtifactForwardedMeta debugArtifactStage = "forwarded_meta"
	debugArtifactPressure      debugArtifactStage = "pressure"
	debugArtifactUsage         debugArtifactStage = "usage"
)

var validDebugArtifactStages = map[debugArtifactStage]struct{}{
	debugArtifactRawBody:       {},
	debugArtifactForwardedBody: {},
	debugArtifactResponseBody:  {},
	debugArtifactRawMetadata:   {},
	debugArtifactForwardedMeta: {},
	debugArtifactPressure:      {},
	debugArtifactUsage:         {},
}

// DebugLayout 集中管理 Debug 根目录、进程 run ID 和 request stage 路由。
// run ID 只在 Server 创建时生成一次，绝不从 wall clock 或 request body 派生。
type DebugLayout struct {
	root       string
	runID      string
	metadataMu sync.Mutex
	openFile   debugFileOpener
}

type debugSessionMetadata struct {
	SessionID   string `json:"session_id"`
	SessionHash string `json:"session_hash"`
}

// debugMetaForWrite 为手工构造的测试元数据补齐当前 Server 的路由身份。
// 真实请求由 nextRequestMeta 预先绑定 RunID/SessionHash；这里不修改调用者对象，
// 避免并发 Debug writer 在补绑定时产生数据竞争，并对跨 Server 串写 fail closed。
func (s *Server) debugMetaForWrite(meta *requestMeta) (*requestMeta, error) {
	if s == nil || s.debugLayout == nil || meta == nil || meta.ID == 0 {
		return nil, fmt.Errorf("debug request meta 未初始化")
	}
	runID := meta.RunID
	if runID == "" {
		runID = s.debugLayout.RunID()
	}
	if runID != s.debugLayout.RunID() || !validLowerHex16(runID) {
		return nil, fmt.Errorf("debug run ID 不匹配")
	}
	sessionHash := meta.SessionHash
	if sessionHash == "" {
		sessionHash = stableSessionHash(meta.RequestSessionID)
	}
	if !validLowerHex16(sessionHash) || sessionHash != stableSessionHash(meta.RequestSessionID) {
		return nil, fmt.Errorf("debug session hash 不匹配")
	}
	return &requestMeta{
		ID:               meta.ID,
		RequestSessionID: meta.RequestSessionID,
		SessionHash:      sessionHash,
		RunID:            runID,
	}, nil
}

func newDebugLayout(dataDir string, random io.Reader) (*DebugLayout, error) {
	if random == nil {
		random = rand.Reader
	}
	root, err := filepath.Abs(filepath.Join(dataDir, "debug"))
	if err != nil {
		return nil, fmt.Errorf("解析 debug 根目录失败: %w", err)
	}
	bytes := make([]byte, debugRunIDBytes)
	if _, err := io.ReadFull(random, bytes); err != nil {
		return nil, fmt.Errorf("生成 debug run ID 失败: %w", err)
	}
	return &DebugLayout{
		root:  root,
		runID: hex.EncodeToString(bytes),
		openFile: func(name string, flag int, perm os.FileMode) (debugWriteCloser, error) {
			return os.OpenFile(name, flag, perm)
		},
	}, nil
}

func (l *DebugLayout) RunID() string {
	if l == nil {
		return ""
	}
	return l.runID
}

func (l *DebugLayout) RequestPath(meta *requestMeta, stage debugArtifactStage) (string, error) {
	if l == nil || l.root == "" || !validLowerHex16(l.runID) {
		return "", fmt.Errorf("debug layout 未初始化")
	}
	if meta == nil || meta.ID == 0 {
		return "", fmt.Errorf("debug request meta 无效")
	}
	if meta.RunID != l.runID || !validLowerHex16(meta.RunID) {
		return "", fmt.Errorf("debug run ID 不匹配")
	}
	sessionHash := meta.SessionHash
	if sessionHash == "" {
		sessionHash = stableSessionHash(meta.RequestSessionID)
	}
	if !validLowerHex16(sessionHash) || sessionHash != stableSessionHash(meta.RequestSessionID) {
		return "", fmt.Errorf("debug session hash 无效")
	}
	if _, ok := validDebugArtifactStages[stage]; !ok || stage == "" {
		return "", fmt.Errorf("debug stage 无效")
	}
	requestDir := filepath.Join(l.root, sessionHash, l.runID, fmt.Sprintf("%d", meta.ID))
	path := filepath.Join(requestDir, string(stage)+".json")
	if !pathWithinRoot(l.root, path) {
		return "", fmt.Errorf("debug path 逃逸根目录")
	}
	return path, nil
}

// EnsureSessionMetadata 对同一 session 根目录最多写入一次 session.json。
// 已存在文件必须能验证出相同身份，否则 fail closed 且不改动旧字节。
func (l *DebugLayout) EnsureSessionMetadata(meta *requestMeta) error {
	if l == nil || meta == nil || meta.RunID != l.runID {
		return fmt.Errorf("debug session metadata 参数无效")
	}
	sessionHash := meta.SessionHash
	if sessionHash == "" {
		sessionHash = stableSessionHash(meta.RequestSessionID)
	}
	if !validLowerHex16(sessionHash) || sessionHash != stableSessionHash(meta.RequestSessionID) || !pathWithinRoot(l.root, filepath.Join(l.root, sessionHash)) {
		return fmt.Errorf("debug session hash 无效")
	}
	sessionDir := filepath.Join(l.root, sessionHash)
	metadataPath := filepath.Join(sessionDir, "session.json")
	if !pathWithinRoot(l.root, metadataPath) {
		return fmt.Errorf("debug session metadata path 逃逸根目录")
	}

	l.metadataMu.Lock()
	defer l.metadataMu.Unlock()
	if err := os.MkdirAll(sessionDir, 0700); err != nil {
		return fmt.Errorf("创建 debug session 目录失败: %w", err)
	}
	payload, err := json.Marshal(debugSessionMetadata{SessionID: meta.RequestSessionID, SessionHash: sessionHash})
	if err != nil {
		return fmt.Errorf("序列化 debug session metadata 失败: %w", err)
	}
	err = writeDebugEntryFile(metadataPath, payload, l.fileOpener())
	if err == nil {
		return nil
	}
	if !os.IsExist(err) {
		return fmt.Errorf("写入 debug session metadata 失败: %w", err)
	}
	existing, readErr := os.ReadFile(metadataPath)
	if readErr != nil {
		return fmt.Errorf("读取既有 debug session metadata 失败: %w", readErr)
	}
	var current debugSessionMetadata
	if json.Unmarshal(existing, &current) != nil || current.SessionHash != sessionHash || current.SessionID != meta.RequestSessionID {
		return fmt.Errorf("既有 debug session metadata 校验失败")
	}
	return nil
}

func (l *DebugLayout) Write(meta *requestMeta, stage debugArtifactStage, data []byte) error {
	if err := l.EnsureSessionMetadata(meta); err != nil {
		return err
	}
	path, err := l.RequestPath(meta, stage)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return fmt.Errorf("创建 debug request 目录失败: %w", err)
	}
	if err := writeDebugEntryFile(path, data, l.fileOpener()); err != nil {
		return fmt.Errorf("写入 debug stage 失败: %w", err)
	}
	return nil
}

func validLowerHex16(value string) bool {
	if len(value) != 16 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func (l *DebugLayout) fileOpener() debugFileOpener {
	if l != nil && l.openFile != nil {
		return l.openFile
	}
	return func(name string, flag int, perm os.FileMode) (debugWriteCloser, error) {
		return os.OpenFile(name, flag, perm)
	}
}

func pathWithinRoot(root, path string) bool {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return false
	}
	pathAbs, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(rootAbs, pathAbs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return false
	}
	return true
}
