package causal

import "sort"

// logEntriesLocked 按统一的 (origin, seq) 序列构造本副本持有、且 seq 大于
// 对应 floor 的全部日志事件。数据操作与成员变更事件交错出现。
func (r *Replica) logEntriesLocked() map[string][]LogEntry {
	out := make(map[string][]LogEntry)
	for id, op := range r.ops {
		out[id.Origin] = append(out[id.Origin], LogEntry{Seq: id.Seq, Op: ptrOp(op)})
	}
	for id, ch := range r.changes {
		out[id.Origin] = append(out[id.Origin], LogEntry{Seq: id.Seq, Change: ptrChange(ch)})
	}
	for origin := range out {
		sort.Slice(out[origin], func(i, j int) bool {
			return out[origin][i].Seq < out[origin][j].Seq
		})
	}
	return out
}

func ptrOp(op Operation) *Operation              { return &op }
func ptrChange(c MemberChangeOp) *MemberChangeOp { return &c }

// EntriesAfter 返回每个 origin 上 seq > since[origin] 的日志事件（深拷贝）。
// 这是增量同步的核心：只传对端缺失的部分，重复调用天然幂等。
func (r *Replica) EntriesAfter(since VersionVector) []LogEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	all := r.logEntriesLocked()
	var out []LogEntry
	origins := make([]string, 0, len(all))
	for origin := range all {
		origins = append(origins, origin)
	}
	sort.Strings(origins)
	for _, origin := range origins {
		for _, e := range all[origin] {
			if e.Seq > since.Get(origin) {
				out = append(out, cloneEntry(e))
			}
		}
	}
	return out
}

// HasEntry 报告是否持有某条日志事件（数据操作或成员变更）。
func (r *Replica) HasEntry(id OpID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.ops[id]; ok {
		return true
	}
	_, ok := r.changes[id]
	return ok
}

func cloneEntry(e LogEntry) LogEntry {
	c := LogEntry{Seq: e.Seq}
	if e.Op != nil {
		op := *e.Op
		op.Value = append([]byte(nil), e.Op.Value...)
		op.Version = e.Op.Version.Clone()
		c.Op = &op
	}
	if e.Change != nil {
		ch := *e.Change
		ch.Members = append([]string(nil), e.Change.Members...)
		ch.Version = e.Change.Version.Clone()
		c.Change = &ch
	}
	return c
}

// ApplyEntries 幂等地应用统一日志事件序列。
//
//   - 重复事件：按 (origin, seq) 去重，绝不重复应用。
//   - 乱序事件：先排序；若存在序号空洞则返回 GapError，稠密前缀已被应用，
//     下一轮同步从水位继续即可（at-least-once + 幂等）。
//   - GC 之后到达的陈旧事件（seq<=gcFloor）：被忽略，旧值不可能复活。
func (r *Replica) ApplyEntries(entries []LogEntry) (newCount int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 去重 + 排序。
	have := make(map[OpID]bool, len(entries))
	sorted := make([]LogEntry, 0, len(entries))
	for _, e := range entries {
		origin := ""
		if e.Op != nil {
			origin = e.Op.Origin
		} else if e.Change != nil {
			origin = e.Change.Origin
		} else {
			continue
		}
		id := OpID{Origin: origin, Seq: e.Seq}
		if have[id] {
			continue
		}
		have[id] = true
		sorted = append(sorted, e)
	}
	sort.Slice(sorted, func(i, j int) bool {
		ai, bi := entryOrigin(sorted[i]), entryOrigin(sorted[j])
		if ai != bi {
			return ai < bi
		}
		return sorted[i].Seq < sorted[j].Seq
	})

	for _, e := range sorted {
		var ok bool
		var applyErr error
		if e.Op != nil {
			ok, applyErr = r.applyOpLocked(*e.Op)
		} else {
			ok, applyErr = r.applyChangeEventLocked(*e.Change)
		}
		if applyErr != nil {
			return newCount, applyErr
		}
		if ok {
			newCount++
		}
	}
	return newCount, nil
}

func entryOrigin(e LogEntry) string {
	if e.Op != nil {
		return e.Op.Origin
	}
	return e.Change.Origin
}
