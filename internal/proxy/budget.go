package proxy

// Budget 管理各代理组件的 token 预算分配。
// 确保重展开 + 检索 + 摘要不超出阈值。
// 对标 YesMem budget.go:1-41。
type Budget struct {
	Narrative     int // 固定 2% 用于摘要文本
	Retrieval     int // 最多 3% 用于关联检索
	ReExpansion   int // 最多 25% 用于重展开桩化消息
	FreshMessages int // 30% 用于 keepRecent 尾部保护消息
	Stubs         int // 剩余空间用于桩化消息

	reExpansionSpent int
	retrievalSpent   int
}

// NewBudget 创建与 token 阈值成正比的预算分配。
func NewBudget(threshold int) *Budget {
	narrative := threshold * 2 / 100    // 2%
	retrieval := threshold * 3 / 100    // 3%
	reExpansion := threshold * 25 / 100 // 25%
	fresh := threshold * 30 / 100       // 30%
	stubs := threshold - narrative - retrieval - reExpansion - fresh // 40%

	return &Budget{
		Narrative:     narrative,
		Retrieval:     retrieval,
		ReExpansion:   reExpansion,
		FreshMessages: fresh,
		Stubs:         stubs,
	}
}

// CanSpendReExpansion 检查重展开预算是否仍有余量容纳 tokens 个 token。
func (b *Budget) CanSpendReExpansion(tokens int) bool {
	return b.reExpansionSpent+tokens <= b.ReExpansion
}

// SpendReExpansion 记录已消费的重展开 token 数。
func (b *Budget) SpendReExpansion(tokens int) {
	b.reExpansionSpent += tokens
}

// CanSpendRetrieval 检查检索预算是否仍有余量容纳 tokens 个 token。
func (b *Budget) CanSpendRetrieval(tokens int) bool {
	return b.retrievalSpent+tokens <= b.Retrieval
}

// SpendRetrieval 记录已消费的检索 token 数。
func (b *Budget) SpendRetrieval(tokens int) {
	b.retrievalSpent += tokens
}
