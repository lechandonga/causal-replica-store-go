package causal

import "sort"

// RecordPeerSeen 记录一次同步中对端自报的 seen 水位（取最大值）。
//
// 关键语义：对端从未见过的 origin 分量按 0 计入，因此必须在“首次收到该成员
// 的自报”时把它登记到所有已知 origin 上（含零值）。否则一个尚未同步过的
// 成员会被错误地当成“对所有 origin 都没有限制”，导致 GC 越权回收。
func (r *Replica) RecordPeerSeen(peerID string, peerSeen VersionVector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.view.contains(peerID) {
		return
	}
	cur, existed := r.peerReported[peerID]
	if !existed {
		cur = make(VersionVector)
		r.peerReported[peerID] = cur
	}
	cur.Merge(peerSeen)
	r.reportedPeers[peerID] = true
	r.recordSelfSeenLocked()
}

// safeWatermarkLocked 计算“所有活跃成员都已见过”的逐 origin 水位。
// 未自报过的成员按 0 处理，即绝不回收未被确认见过的操作。
func (r *Replica) safeWatermarkLocked() VersionVector {
	safe := make(VersionVector)
	if len(r.view.Members) == 0 {
		return safe
	}
	first := true
	for _, m := range r.view.Members {
		if !r.reportedPeers[m] {
			// 存在从未同步过的成员：什么都不能回收。
			return VersionVector{}
		}
		reported := r.peerReported[m]
		// 以本副本见过的所有 origin 为底；成员未报告的 origin 缺省为 0。
		origins := make(map[string]struct{}, len(r.seen))
		for k := range r.seen {
			origins[k] = struct{}{}
		}
		for k := range safe {
			origins[k] = struct{}{}
		}
		for k := range reported {
			origins[k] = struct{}{}
		}
		if first {
			for k := range origins {
				safe[k] = reported[k]
			}
			first = false
			continue
		}
		for k := range origins {
			if v := reported[k]; v < safe[k] {
				safe[k] = v
			}
		}
	}
	return safe
}

// GCReport 描述一次垃圾回收的结果。
type GCReport struct {
	// Reaped 是真正被物理删除的操作。
	Reaped []OpID
	// SafeWatermark 是本次确认的“全体成员已见”水位。
	SafeWatermark VersionVector
}

// CollectGarbage 在保证安全的前提下回收操作与墓碑。
//
// 回收规则：
//  1. 只有所有活跃成员都自报见过的操作才允许回收（防止旧值复活所需的
//     因果历史丢失）。
//  2. 墓碑只有在支配该键的全部存活操作（即删除已经确定胜出）时才回收；
//     “值与墓碑并发”的墓碑会被保留，直到冲突被显式解决。
//  3. 回收水位 gcFloor 单调递增；回收之后，任何 seq<=gcFloor 的迟到/重传
//     消息都会被幂等忽略，而不是重新应用。
func (r *Replica) CollectGarbage() GCReport {
	r.mu.Lock()
	defer r.mu.Unlock()

	safe := r.safeWatermarkLocked()
	report := GCReport{SafeWatermark: safe.Clone()}

	// 逐键判断墓碑是否已确定胜出。
	tombstoneWins := make(map[OpID]bool)
	byKey := make(map[string][]Operation)
	for _, op := range r.ops {
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	for key, group := range byKey {
		_ = key
		for _, t := range group {
			if t.Kind != OpDelete {
				continue
			}
			wins := true
			for _, other := range group {
				if t.ID() == other.ID() {
					continue
				}
				// 任何不被该墓碑支配的兄弟操作存在，墓碑就不能回收。
				if !t.Version.Dominates(other.Version, true) {
					wins = false
					break
				}
			}
			tombstoneWins[t.ID()] = wins
		}
	}

	for id, op := range r.ops {
		w := safe.Get(op.Origin)
		if op.Seq > w {
			continue // 还有成员没见过，绝不回收
		}
		if op.Kind == OpDelete && !tombstoneWins[id] {
			continue // 删除尚未确定胜出（仍有并发值），保留墓碑
		}
		delete(r.ops, id)
		report.Reaped = append(report.Reaped, id)
	}

	// 抬升回收水位（单调不减）。
	for origin, w := range safe {
		if w > r.gcFloor[origin] {
			r.gcFloor[origin] = w
		}
	}

	sort.Slice(report.Reaped, func(i, j int) bool {
		if report.Reaped[i].Origin != report.Reaped[j].Origin {
			return report.Reaped[i].Origin < report.Reaped[j].Origin
		}
		return report.Reaped[i].Seq < report.Reaped[j].Seq
	})

	// 成员变更事件也在统一序列上：一旦全体成员都见过即可回收（成员视图本身
	// 已经反映了最新纪元与成员列表，回收事件不会改变可见状态）。
	for id, ch := range r.changes {
		if ch.Seq > safe.Get(ch.Origin) {
			continue
		}
		delete(r.changes, id)
		report.Reaped = append(report.Reaped, id)
	}
	sort.Slice(report.Reaped, func(i, j int) bool {
		if report.Reaped[i].Origin != report.Reaped[j].Origin {
			return report.Reaped[i].Origin < report.Reaped[j].Origin
		}
		return report.Reaped[i].Seq < report.Reaped[j].Seq
	})
	return report
}

// PeerNeedsSnapshot 报告“对端”是否已落后到无法增量同步：
// 仅当对端自报的 seen 低于本副本的 GC 回收水位（中间操作已被物理回收）
// 时才成立。成员纪元落后不需要快照——成员变更事件本身就是统一日志上的
// 增量事件，可正常补齐。
func (r *Replica) PeerNeedsSnapshot(peerSeen VersionVector, peerMembership Membership) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for origin, floor := range r.gcFloor {
		if floor == 0 {
			continue
		}
		if peerSeen.Get(origin) < floor {
			return true
		}
	}
	return false
}
