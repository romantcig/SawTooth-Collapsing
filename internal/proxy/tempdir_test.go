package proxy

import (
	"os"
	"strings"
	"testing"
	"time"
)

// tempDirCleanupAttempts 是删除临时目录的最大尝试次数。
const tempDirCleanupAttempts = 10

// tempDirRetryCleanup 创建测试专用临时目录，并在测试结束后带退避重试地删除它。
//
// 为什么不直接用 t.TempDir()：
// Windows 上的删除是延迟生效的——只有当一个文件的所有句柄（含 Defender、搜索索引器
// 在新建文件上打开的瞬时句柄）都关闭后，它的目录项才真正消失。os.RemoveAll 先逐个删
// 文件、再对目录做 rmdir，此刻目录项可能还没消失，于是撞上 ERROR_DIR_NOT_EMPTY
// （145，"The directory is not empty"）。而标准库 testing 的 Windows 重试白名单
// （testing_windows.go 的 isWindowsRetryable）只认 ERROR_ACCESS_DENIED 与
// ERROR_SHARING_VIOLATION，不含 145 —— 所以 t.TempDir() 注册的 cleanup 一次失败
// 都不重试，直接 Errorf，把断言全绿的测试染红。
//
// 因此这里自己做退避重试；即使最终仍删不掉，也只留日志、不判红：
// 清理残留不是被测行为，一次清理事故不该改变测试结论。
func tempDirRetryCleanup(t *testing.T) string {
	t.Helper()

	// 子测试名可能携带路径分隔符与 Windows 文件名非法字符（如路径逃逸测试的
	// "..\\escape"、"C:\escape" 子测试名），而 os.MkdirTemp 的 pattern 一旦含
	// 路径分隔符会直接报错；统一下划线化后，残留目录仍能靠名字定位到是哪个测试留下的。
	name := strings.Map(func(r rune) rune {
		switch r {
		case '/', '\\', ':', '"', '<', '>', '|', '?', '*':
			return '_'
		}
		return r
	}, t.Name())
	dir, err := os.MkdirTemp("", "sawtooth-"+name+"-")
	if err != nil {
		// 创建不出临时目录是真实故障，必须判红。
		t.Fatalf("创建临时目录失败: %v", err)
	}

	t.Cleanup(func() {
		var lastErr error
		for attempt := 1; attempt <= tempDirCleanupAttempts; attempt++ {
			if lastErr = os.RemoveAll(dir); lastErr == nil {
				return
			}
			if attempt == tempDirCleanupAttempts {
				break
			}
			// 只在失败时退避，间隔随尝试次数线性增长。
			time.Sleep(time.Duration(attempt) * 20 * time.Millisecond)
		}
		// 只留痕，不改变测试结论——残留路径必须可见，不能被静默吞掉。
		t.Logf("临时目录清理失败，已放弃（%d 次尝试）: %s: %v",
			tempDirCleanupAttempts, dir, lastErr)
	})

	return dir
}
