package proxy

import (
	"testing"
)

func TestNewBudget(t *testing.T) {
	b := NewBudget(100000)

	if b.Narrative != 2000 {
		t.Errorf("Narrative = %d, want 2000 (2%%)", b.Narrative)
	}
	if b.Retrieval != 3000 {
		t.Errorf("Retrieval = %d, want 3000 (3%%)", b.Retrieval)
	}
	if b.ReExpansion != 25000 {
		t.Errorf("ReExpansion = %d, want 25000 (25%%)", b.ReExpansion)
	}
	if b.FreshMessages != 30000 {
		t.Errorf("FreshMessages = %d, want 30000 (30%%)", b.FreshMessages)
	}
	// Stubs = 100000 - 2000 - 3000 - 25000 - 30000 = 40000
	if b.Stubs != 40000 {
		t.Errorf("Stubs = %d, want 40000 (40%%)", b.Stubs)
	}
}

func TestBudgetCanSpendReExpansion(t *testing.T) {
	b := NewBudget(100000)

	if !b.CanSpendReExpansion(25000) {
		t.Error("should be able to spend 25000 (exactly at limit)")
	}
	if b.CanSpendReExpansion(25001) {
		t.Error("should NOT be able to spend 25001 (over limit)")
	}

	b.SpendReExpansion(20000)
	if !b.CanSpendReExpansion(5000) {
		t.Error("should be able to spend 5000 more (total 25000)")
	}
	if b.CanSpendReExpansion(5001) {
		t.Error("should NOT be able to spend 5001 more (total 25001)")
	}
}

func TestBudgetSpendReExpansion(t *testing.T) {
	b := NewBudget(100000)

	b.SpendReExpansion(10000)
	b.SpendReExpansion(5000)

	if !b.CanSpendReExpansion(10000) {
		t.Error("should be able to spend 10000 more (total 25000)")
	}

	b.SpendReExpansion(10000)
	// 10000 + 5000 + 10000 = 25000 = ReExpansion limit
	if b.CanSpendReExpansion(1) {
		t.Error("should have exhausted re-expansion budget")
	}
}

func TestBudgetRemainingReExpansion(t *testing.T) {
	b := &Budget{ReExpansion: 100}

	if got := b.RemainingReExpansion(); got != 100 {
		t.Fatalf("initial remaining = %d, want 100", got)
	}
	b.SpendReExpansion(35)
	if got := b.RemainingReExpansion(); got != 65 {
		t.Fatalf("remaining after spend = %d, want 65", got)
	}
	b.SpendReExpansion(100)
	if got := b.RemainingReExpansion(); got != 0 {
		t.Fatalf("overspent remaining = %d, want saturated 0", got)
	}
}

func TestBudgetCanSpendRetrieval(t *testing.T) {
	b := NewBudget(100000)

	if !b.CanSpendRetrieval(3000) {
		t.Error("should be able to spend 3000 (exactly at limit)")
	}
	if b.CanSpendRetrieval(3001) {
		t.Error("should NOT be able to spend 3001 (over limit)")
	}

	b.SpendRetrieval(2000)
	if !b.CanSpendRetrieval(1000) {
		t.Error("should be able to spend 1000 more")
	}
}

func TestBudgetSpendRetrieval(t *testing.T) {
	b := NewBudget(100000)

	b.SpendRetrieval(1000)
	b.SpendRetrieval(1000)

	if !b.CanSpendRetrieval(1000) {
		t.Error("should be able to spend 1000 more (total 3000)")
	}

	b.SpendRetrieval(1000)
	if b.CanSpendRetrieval(1) {
		t.Error("should have exhausted retrieval budget")
	}
}

func TestBudgetDefaultThreshold(t *testing.T) {
	// 使用默认 token threshold 100000
	b := NewBudget(100000)

	total := b.Narrative + b.Retrieval + b.ReExpansion + b.FreshMessages + b.Stubs
	if total != 100000 {
		t.Errorf("budget components sum to %d, want 100000", total)
	}
}

// TestBudget_ScalesWithThreshold 验证 Budget 随阈值按比例缩放。
// 对标 YesMem budget_test.go:37-45。
func TestBudget_ScalesWithThreshold(t *testing.T) {
	b := NewBudget(200000)

	if b.ReExpansion != 50000 {
		t.Errorf("ReExpansion = %d, want 50000 for threshold=200000", b.ReExpansion)
	}
	if b.Narrative != 4000 {
		t.Errorf("Narrative = %d, want 4000 for threshold=200000", b.Narrative)
	}
	if b.Retrieval != 6000 {
		t.Errorf("Retrieval = %d, want 6000 for threshold=200000", b.Retrieval)
	}

	total := b.Narrative + b.Retrieval + b.ReExpansion + b.FreshMessages + b.Stubs
	if total != 200000 {
		t.Errorf("budget components sum to %d, want 200000", total)
	}
}

// TestBudget_SmallThreshold 验证小阈值下无负值、求和正确。
// 对标 YesMem budget_test.go:66-76。
func TestBudget_SmallThreshold(t *testing.T) {
	b := NewBudget(10000)

	// 无负值
	if b.Narrative < 0 || b.Retrieval < 0 || b.ReExpansion < 0 ||
		b.FreshMessages < 0 || b.Stubs < 0 {
		t.Error("all budget fields must be non-negative")
	}

	total := b.Narrative + b.Retrieval + b.ReExpansion + b.FreshMessages + b.Stubs
	if total != 10000 {
		t.Errorf("budget components sum to %d, want 10000", total)
	}
}

// TestBudget_ZeroThreshold 验证零阈值边界。
func TestBudget_ZeroThreshold(t *testing.T) {
	b := NewBudget(0)

	// 各字段应为 0
	if b.Narrative != 0 || b.Retrieval != 0 || b.ReExpansion != 0 ||
		b.FreshMessages != 0 || b.Stubs != 0 {
		t.Error("all budget fields must be 0 for threshold=0")
	}

	// 无预算时应拒绝任何支出
	if b.CanSpendReExpansion(1) {
		t.Error("should not be able to spend with zero budget")
	}
	if b.CanSpendRetrieval(1) {
		t.Error("should not be able to spend retrieval with zero budget")
	}
}
