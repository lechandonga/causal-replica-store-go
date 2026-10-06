package causal

import (
	"errors"
	"sort"
)

// ErrConflict 表示该键存在并发冲突，需要显式解决。
var ErrConflict = errors.New("causal: 键存在并发冲突，请用 View 读取全部并发版本后显式 Resolve")

// ErrNotFound 表示键不存在（已删除或从未写入）。
var ErrNotFound = errors.New("causal: 键不存在")

// applyOpLocked 幂等地应用一个通过同步到达的操作。
// 序号必须稠密：来自同一 origin 的 seq 不允许跳号。
func (r *Replica) applyOpLocked(op Operation) (bool, error) {
	id := op.ID()
	if _, ok := r.ops[id]; ok {
		return false, nil
	}
	floor := r.gcFloor.Get(op.Origin)
	if op.Seq <= floor {
		// 已被 GC 的操作重复到达：忽略，且不移动任何水位。
		return false, nil
	}
	if op.Seq != r.seen.Get(op.Origin)+1 {
		return false, &GapError{Origin: op.Origin, Have: r.seen.Get(op.Origin), Got: op.Seq}
	}
	r.ops[id] = op
	r.seen[op.Origin] = op.Seq
	return true, nil
}

// GapError 表示收到的操作在序号上出现空洞，需要对端重传缺失操作。
type GapError struct {
	Origin string
	Have   uint64
	Got    uint64
}

func (e *GapError) Error() string {
	return "causal: 操作序号空洞（乱序消息先到）"
}

// viewLocked 计算某个键当前互不可支配的存活版本集合。
func (r *Replica) viewLocked(key string) (KeyView, error) {
	var live []Operation
	var tombstones []Operation
	for _, op := range r.ops {
		if op.Key != key {
			continue
		}
		if op.Kind == OpDelete {
			tombstones = append(tombstones, op)
		} else {
			live = append(live, op)
		}
	}

	// 被更新墓碑支配的旧墓碑不再可见；只要仍有存活墓碑，键即为已删除。
	hasTomb := false
	for i, t := range tombstones {
		dominated := false
		for j, t2 := range tombstones {
			if i == j {
				continue
			}
			if t2.Version.Dominates(t.Version, false) {
				dominated = true
				break
			}
		}
		if !dominated {
			hasTomb = true
		}
	}

	// 被墓碑因果支配的旧 PUT 被删除淘汰；乱序迟到的旧值永远不会复活新值。
	var survivingPuts []Operation
	for _, p := range live {
		killed := false
		for _, t := range tombstones {
			if t.Version.Dominates(p.Version, false) {
				killed = true
				break
			}
		}
		if !killed {
			survivingPuts = append(survivingPuts, p)
		}
	}

	// 存活 PUT 之间去支配，留下真正并发的版本。
	var entries []ConflictEntry
	for i, p := range survivingPuts {
		dominated := false
		for j, q := range survivingPuts {
			if i == j {
				continue
			}
			if q.Version.Dominates(p.Version, false) {
				dominated = true
				break
			}
		}
		if !dominated {
			entries = append(entries, ConflictEntry{
				Origin:  p.Origin,
				Seq:     p.Seq,
				Value:   append([]byte(nil), p.Value...),
				Version: p.Version.Clone(),
			})
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Origin != entries[j].Origin {
			return entries[i].Origin < entries[j].Origin
		}
		return entries[i].Seq < entries[j].Seq
	})

	// 冲突判定：多个并发 PUT，或存活 PUT 与存活墓碑并发（删除/更新竞争）。
	conflict := len(entries) > 1 || (hasTomb && len(entries) > 0)

	return KeyView{
		Key:          key,
		Entries:      entries,
		HasTombstone: hasTomb,
		Conflict:     conflict,
	}, nil
}

// View 返回某键的完整视图（确定性排序）。
func (r *Replica) View(key string) (KeyView, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.viewLocked(key)
}

// Get 读取单个确定值。存在冲突时返回 ErrConflict；无存活值时返回 ErrNotFound。
func (r *Replica) Get(key string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, err := r.viewLocked(key)
	if err != nil {
		return nil, err
	}
	if v.Conflict {
		return nil, ErrConflict
	}
	if len(v.Entries) == 0 {
		return nil, ErrNotFound
	}
	return append([]byte(nil), v.Entries[0].Value...), nil
}

// Keys 返回当前有存活值的全部键（确定性字典序；墓碑淘汰的键不出现）。
func (r *Replica) Keys() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	set := make(map[string]struct{})
	for _, op := range r.ops {
		if op.Kind == OpPut {
			set[op.Key] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		v, err := r.viewLocked(k)
		if err == nil && len(v.Entries) > 0 {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// Seen 返回各 origin 已知最大操作序号的副本（用于同步摘要与断言）。
func (r *Replica) Seen() VersionVector {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seen.Clone()
}

// GCFloor 返回当前回收水位。
func (r *Replica) GCFloor() VersionVector {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.gcFloor.Clone()
}

// Membership 返回当前成员视图。
func (r *Replica) Membership() Membership {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.view.clone()
}

// OpCount 返回当前保存的数据操作条数（测试/可观测用）。
func (r *Replica) OpCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ops)
}

// FindOp 返回指定操作的副本。
func (r *Replica) FindOp(id OpID) (Operation, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	op, ok := r.ops[id]
	if ok {
		op.Value = append([]byte(nil), op.Value...)
		op.Version = op.Version.Clone()
	}
	return op, ok
}
