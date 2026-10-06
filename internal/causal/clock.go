package causal

import (
	"fmt"
	"strings"
)

// VersionVector 是一个副本集合上的因果计数器。
// vv[replicaID] 表示该副本当前已知的由该 replicaID 发起的操作计数。
type VersionVector map[string]uint64

// Clone 返回向量的深拷贝。
func (vv VersionVector) Clone() VersionVector {
	out := make(VersionVector, len(vv))
	for k, v := range vv {
		out[k] = v
	}
	return out
}

// Get 安全读取某个副本的计数。
func (vv VersionVector) Get(replicaID string) uint64 {
	return vv[replicaID]
}

// Merge 将 other 合并进自身（逐分量取最大值）。
func (vv VersionVector) Merge(other VersionVector) {
	for k, v := range other {
		if v > vv[k] {
			vv[k] = v
		}
	}
}

// Dominates 判断 vv 是否因果上支配 other：
//   - 逐分量 vv[x] >= other[x]
//   - 且至少有一个分量严格大于（equal=false 时要求严格支配）
//
// equal=true 时相等也算“支配”，用于判断一条操作是否已被包含。
func (vv VersionVector) Dominates(other VersionVector, equal bool) bool {
	strict := false
	for k, v := range other {
		cur := vv[k]
		if cur < v {
			return false
		}
		if cur > v {
			strict = true
		}
	}
	if equal {
		return true
	}
	return strict
}

// ConcurrentWith 判断两个向量是否并发（互不可支配）。
func (vv VersionVector) ConcurrentWith(other VersionVector) bool {
	return !vv.Dominates(other, true) && !other.Dominates(vv, true)
}

// Compare 比较两个操作版本的因果关系：
//
//	-1 表示 vv 在 other 之前（被支配）
//	 0 表示二者并发或相等（调用方可用 equal 进一步区分）
//	+1 表示 vv 在 other 之后（支配）
func (vv VersionVector) Compare(other VersionVector) (cmp int, equal bool) {
	aDom := vv.Dominates(other, false)
	bDom := other.Dominates(vv, false)
	switch {
	case aDom:
		return 1, false
	case bDom:
		return -1, false
	default:
		return 0, vv.Dominates(other, true) && other.Dominates(vv, true)
	}
}

func (vv VersionVector) String() string {
	parts := make([]string, 0, len(vv))
	for k, v := range vv {
		parts = append(parts, fmt.Sprintf("%s:%d", k, v))
	}
	return "{" + strings.Join(parts, ",") + "}"
}
