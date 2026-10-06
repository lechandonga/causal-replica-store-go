package causal

import (
	"errors"
	"time"
)

// SyncKind 是同步消息类型。
type SyncKind int

const (
	// MsgHello 同步握手：携带本副本的成员视图、seen 摘要与 gcFloor。
	MsgHello SyncKind = iota + 1
	// MsgData 携带统一日志事件（数据操作 + 成员变更），幂等应用。
	MsgData
	// MsgSnapshotRequired 告知对端：你的水位低于我的回收水位/成员纪元，
	// 增量同步无法收敛，请接收全量快照。
	MsgSnapshotRequired
	// MsgSnapshot 全量快照（确定性的“回收后离线副本回归”处理）。
	MsgSnapshot
)

// SyncMessage 是副本间交换的消息。
type SyncMessage struct {
	Kind       SyncKind
	From       string
	To         string
	Membership Membership
	Seen       VersionVector
	GCFloor    VersionVector
	Entries    []LogEntry
	Snapshot   *Snapshot
	// SessionID 仅用于日志排障；幂等性不依赖它。
	SessionID int64
}

// SyncResult 是一轮双向同步的结果统计。
type SyncResult struct {
	AppliedByA  int
	AppliedByB  int
	SnapshotToA bool
	SnapshotToB bool
	RetiredPeer string
	Rounds      int
	Converged   bool
	peerA       *Replica
}

// ExchangeOnce 执行一次双向状态交换（不发送 Hello，直接按各自水位互推）。
// 它是幂等的：重复调用只会重发/重复应用，ApplyEntries 负责去重。
// 返回双方新应用的事件数。
func ExchangeOnce(a, b *Replica) (appliedA, appliedB int, err error) {
	aSnap := a.GetSnapshot()
	bSnap := b.GetSnapshot()

	// 纪元落后或低于回收水位 => 必须快照重同步，旧身份的增量日志直接丢弃。
	if a.PeerNeedsSnapshot(bSnap.Seen, bSnap.Membership) {
		a.RestoreFromSnapshot(bSnap)
		a.RecordPeerSeen(b.ID(), bSnap.Seen)
		b.RecordPeerSeen(a.ID(), a.Seen())
		return 0, 0, nil
	}
	if b.PeerNeedsSnapshot(aSnap.Seen, aSnap.Membership) {
		b.RestoreFromSnapshot(aSnap)
		b.RecordPeerSeen(a.ID(), aSnap.Seen)
		a.RecordPeerSeen(b.ID(), b.Seen())
		return 0, 0, nil
	}

	entriesToB := a.EntriesAfter(bSnap.Seen)
	entriesToA := b.EntriesAfter(aSnap.Seen)

	if n, e := b.ApplyEntries(entriesToB); e == nil {
		appliedB = n
	} else {
		// 空洞（乱序先到）：本轮只应用稠密前缀，下一轮补齐。
		var gap *GapError
		if errors.As(e, &gap) {
			appliedB = n
		} else {
			return 0, 0, e
		}
	}
	if n, e := a.ApplyEntries(entriesToA); e == nil {
		appliedA = n
	} else {
		var gap *GapError
		if errors.As(e, &gap) {
			appliedA = n
		} else {
			return 0, 0, e
		}
	}

	// 交换成员视图（高纪元胜出已在 ApplyEntries 中完成；这里再合并元数据）。
	a.RecordPeerSeen(b.ID(), b.Seen())
	b.RecordPeerSeen(a.ID(), a.Seen())

	return appliedA, appliedB, nil
}

// SyncToConvergence 反复双向交换，直到双方可见事件集合一致，或达到 maxRounds。
// 每一轮都是完整幂等的，因此在丢包/乱序/重复/断连场景下反复调用即可收敛。
func SyncToConvergence(a, b *Replica, maxRounds int) (SyncResult, error) {
	res := SyncResult{Rounds: 0}
	for i := 0; i < maxRounds; i++ {
		res.Rounds++
		beforeA := a.Seen()
		beforeB := b.Seen()
		na, nb, err := ExchangeOnce(a, b)
		if err != nil {
			return res, err
		}
		res.AppliedByA += na
		res.AppliedByB += nb

		afterA := a.Seen()
		afterB := b.Seen()
		sameA := vectorsEqual(beforeA, afterA)
		sameB := vectorsEqual(beforeB, afterB)

		// 收敛判据：双方 seen 完全相同，且成员视图纪元一致。
		if sameA && sameB && vectorsEqual(afterA, afterB) &&
			a.Membership().Epoch == b.Membership().Epoch {
			res.Converged = true
			return res, nil
		}
	}
	return res, errors.New("causal: 达到最大同步轮数仍未收敛")
}

func vectorsEqual(x, y VersionVector) bool {
	if len(x) != len(y) {
		return false
	}
	for k, v := range x {
		if y.Get(k) != v {
			return false
		}
	}
	return true
}

// DriveSyncOverNetwork 通过不可靠消息网络驱动一对副本同步到收敛。
//
// 每“跳”双方互发 Hello，随后互推缺失事件；任何丢失都会在后续跳中被重传，
// 重复事件由 ApplyEntries 去重，乱序事件由稠密序号 + GapError 处理，
// 断连期间直接跳过发送（消息丢失），调用方在 Reconnect 后再次调用本函数即可。
func DriveSyncOverNetwork(net *Network, a, b *Replica, maxHops int) (SyncResult, error) {
	res := SyncResult{}
	res.peerA = a
	for hop := 0; hop < maxHops; hop++ {
		session := time.Now().UnixNano()
		// 每跳执行两个相位，保证“对端收到我的数据后回推其水位”在同跳内闭环：
		// phase1 互报水位 -> 投递处理 -> phase2 按最新水位互推 -> 投递处理 -> 判收敛。
		pushPhase := func() {
			as := a.GetSnapshot()
			bs := b.GetSnapshot()
			push := func(sender *Replica, toID string, selfSnap, peerSnap Snapshot) {
				if sender.PeerNeedsSnapshot(peerSnap.Seen, peerSnap.Membership) {
					snap := sender.GetSnapshot()
					net.Send(sender.ID(), toID, SyncMessage{
						Kind: MsgSnapshotRequired, From: sender.ID(), To: toID,
						Membership: snap.Membership, Seen: snap.Seen, GCFloor: snap.GCFloor,
						Snapshot: &snap, SessionID: session,
					})
					return
				}
				entries := sender.EntriesAfter(peerSnap.Seen)
				if len(entries) == 0 {
					// 空增量也发一个轻量 Hello，传播最新 seen 水位（GC 判定需要）。
					net.Send(sender.ID(), toID, SyncMessage{
						Kind: MsgHello, From: sender.ID(), To: toID, SessionID: session,
						Membership: selfSnap.Membership, Seen: selfSnap.Seen, GCFloor: selfSnap.GCFloor,
					})
					return
				}
				net.Send(sender.ID(), toID, SyncMessage{
					Kind: MsgData, From: sender.ID(), To: toID,
					Membership: selfSnap.Membership, Seen: selfSnap.Seen,
					Entries: entries, SessionID: session,
				})
			}
			push(a, b.ID(), as, bs)
			push(b, a.ID(), bs, as)
		}
		processBox := func(r *Replica, peer *Replica) {
			for _, m := range net.DrainPending(r.ID()) {
				r.applyNetworkMessage(m, peer, &res)
			}
		}

		pushPhase()
		processBox(a, b)
		processBox(b, a)
		pushPhase()
		processBox(a, b)
		processBox(b, a)

		if vectorsEqual(a.Seen(), b.Seen()) &&
			a.Membership().Epoch == b.Membership().Epoch {
			// 必须确认对方收件箱里没有待处理的迟到数据，避免假收敛。
			if net.Pending(a.ID()) == 0 && net.Pending(b.ID()) == 0 {
				res.Converged = true
				res.Rounds = hop + 1
				return res, nil
			}
		}
	}
	return res, errors.New("causal: 网络同步达到最大跳数仍未收敛（可能链路持续断开）")
}

func (r *Replica) applyNetworkMessage(m SyncMessage, peer *Replica, res *SyncResult) {
	switch m.Kind {
	case MsgHello:
		r.RecordPeerSeen(m.From, m.Seen)
	case MsgData:
		// 即使携带的成员视图更新，事件中也包含了变更事件，统一幂等应用。
		n, _ := r.ApplyEntries(m.Entries)
		if r.ID() < peer.ID() {
			res.AppliedByA += n
		} else {
			res.AppliedByB += n
		}
		r.RecordPeerSeen(m.From, m.Seen)
	case MsgSnapshotRequired, MsgSnapshot:
		if m.Snapshot != nil {
			if r == res.peerA {
				res.SnapshotToA = true
			} else {
				res.SnapshotToB = true
			}
			r.RestoreFromSnapshot(*m.Snapshot)
		}
		r.RecordPeerSeen(m.From, m.Seen)
	}
}
