package causal

import (
	"fmt"
	"sort"
)

// 成员变更与数据操作共享 (origin, seq) 序号空间：
// 对 origin X 而言，它的日志是“数据操作与成员变更事件”的一条统一稠密序列。
// 这样同步协议只需按一个 seq 游标推进，乱序、丢失、重传的处理完全一致。

func (r *Replica) issueChangeLocked(kind ChangeKind, targetID string) MemberChangeOp {
	seq, ver := r.bumpLocal()
	members := append([]string(nil), r.view.Members...)
	switch kind {
	case ChangeAdd:
		if !r.view.contains(targetID) {
			members = append(members, targetID)
		}
	case ChangeRetire:
		filtered := members[:0]
		for _, m := range members {
			if m != targetID {
				filtered = append(filtered, m)
			}
		}
		members = filtered
	}
	sort.Strings(members)
	op := MemberChangeOp{
		Origin:   r.id,
		Seq:      seq,
		Kind:     kind,
		TargetID: targetID,
		Epoch:    r.view.Epoch + 1,
		Members:  members,
		Version:  ver,
		WallTime: int64(ver[r.id]),
	}
	r.changes[op.ID()] = op
	r.applyChangeLocked(op)
	// 本地发起的变更必须立即生效，不依赖“纪元更高”这种可能被同跳
	// 其它事件干扰的判断。
	r.view = Membership{Epoch: op.Epoch, Members: append([]string(nil), op.Members...)}
	sort.Strings(r.view.Members)
	return op
}

func (r *Replica) applyChangeLocked(op MemberChangeOp) {
	if op.Epoch > r.view.Epoch {
		r.view = Membership{Epoch: op.Epoch, Members: append([]string(nil), op.Members...)}
		sort.Strings(r.view.Members)
	}
	for m := range r.peerReported {
		if !r.view.contains(m) {
			delete(r.peerReported, m)
			delete(r.reportedPeers, m)
		}
	}
}

// AddMember 向集群加入一个成员（其 ID 先以元数据方式告知）。
func (r *Replica) AddMember(targetID string) (MemberChangeOp, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.view.contains(r.id) {
		return MemberChangeOp{}, fmt.Errorf("causal: 副本 %q 已退役，不能发起成员变更", r.id)
	}
	if r.view.contains(targetID) {
		return MemberChangeOp{}, fmt.Errorf("causal: %q 已是当前成员", targetID)
	}
	return r.issueChangeLocked(ChangeAdd, targetID), nil
}

// RetireMember 永久退役一个成员。
//
// 退役事件因果复制，保证所有存活副本对成员集合达成一致，GC 的全员确认
// 不会被一个永不回来的成员卡死。被退役副本日后回归时，同步协议会检测到
// 其成员纪元落后，执行确定性的快照重同步，并且它不能用旧身份重新写入。
func (r *Replica) RetireMember(targetID string) (MemberChangeOp, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.view.contains(r.id) {
		return MemberChangeOp{}, fmt.Errorf("causal: 副本 %q 已退役，不能发起成员变更", r.id)
	}
	if !r.view.contains(targetID) {
		return MemberChangeOp{}, fmt.Errorf("causal: %q 不是当前成员", targetID)
	}
	// 退役前必须确认目标副本不会再有“集群未知”的本地操作游离在外。
	// 判定方式：目标必须至少自报过一次水位（与其直接或间接同步过），
	// 否则它可能持有从未传播的写入，退役将造成永久丢失。
	if targetID != r.id && !r.reportedPeers[targetID] {
		return MemberChangeOp{}, fmt.Errorf("causal: 从未收到 %q 的水位自报，不能退役", targetID)
	}
	return r.issueChangeLocked(ChangeRetire, targetID), nil
}

// applyChangeEventLocked 幂等应用一个通过同步到达的成员变更事件。
// 必须已持有该 origin 稠密序列上的前一个事件/操作。
func (r *Replica) applyChangeEventLocked(op MemberChangeOp) (bool, error) {
	if _, ok := r.changes[op.ID()]; ok {
		return false, nil
	}
	floor := r.gcFloor.Get(op.Origin)
	if op.Seq <= floor {
		return false, nil
	}
	if op.Seq != r.seen.Get(op.Origin)+1 {
		return false, &GapError{Origin: op.Origin, Have: r.seen.Get(op.Origin), Got: op.Seq}
	}
	r.changes[op.ID()] = op
	r.seen[op.Origin] = op.Seq
	r.applyChangeLocked(op)
	return true, nil
}

// Changes 返回已知成员变更事件（按纪元排序）。
func (r *Replica) Changes() []MemberChangeOp {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]MemberChangeOp, 0, len(r.changes))
	for _, op := range r.changes {
		out = append(out, op)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Epoch != out[j].Epoch {
			return out[i].Epoch < out[j].Epoch
		}
		if out[i].Origin != out[j].Origin {
			return out[i].Origin < out[j].Origin
		}
		return out[i].Seq < out[j].Seq
	})
	return out
}

// IsRetired 报告本副本是否已从成员视图中退役。
func (r *Replica) IsRetired() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.view.contains(r.id)
}
