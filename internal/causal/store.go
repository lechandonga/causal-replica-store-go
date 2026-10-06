// Package causal 实现一个纯本地、不依赖任何外部服务的多副本因果 KV 存储。
//
// 核心抽象：
//   - [Replica]：单个副本，维护操作日志、键索引、GC 水位与成员视图。
//   - [Network]：进程内消息通道模拟的不可靠网络，支持丢包、重复、乱序与断连。
//   - [Driver]：在不可靠网络上驱动幂等的增量同步与快照重同步。
package causal

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

// ConflictEntry 是一次冲突读取中暴露出来的单个并发版本。
type ConflictEntry struct {
	Origin  string
	Seq     uint64
	Deleted bool
	Value   []byte
	Version VersionVector
}

// KeyView 是某个键在当前副本上的完整可见视图。
type KeyView struct {
	Key string
	// Entries 是相互并发（互不可支配）的存活 PUT 版本；若只剩墓碑则为空。
	Entries []ConflictEntry
	// HasTombstone 表示存在尚未被支配的删除版本。
	HasTombstone bool
	// Conflict 为 true 时读取方必须显式 Resolve 才能继续“无冲突写”。
	Conflict bool
}

// Replica 是一个因果 KV 副本。
type Replica struct {
	mu sync.Mutex

	id   string
	view Membership

	// 数据操作日志，按 (origin, seq) 唯一；seq 在每个 origin 上稠密（无空洞）。
	ops map[OpID]Operation
	// 每个 origin 已收到的最大 seq。
	seen VersionVector
	// 每个 origin 已 GC 回收掉的最大 seq（回收水位，单调不减）。
	gcFloor VersionVector
	// 本地发起的序号。
	localSeq uint64

	// 成员变更控制事件（与数据操作共享序号空间）。
	changes map[OpID]MemberChangeOp

	// 最近一次各成员在同步时自报的 seen 水位，用于 GC 安全判定。
	peerReported map[string]VersionVector
	// reportedPeers 记录哪些成员至少自报过一次（区分“从未同步”与“同步过但水位为 0”）。
	reportedPeers map[string]bool
}

// NewReplica 创建一个副本。initialMembers 为初始固定成员集合。
func NewReplica(id string, initialMembers []string) *Replica {
	mems := append([]string(nil), initialMembers...)
	sort.Strings(mems)
	r := &Replica{
		id:            id,
		view:          Membership{Epoch: 1, Members: mems},
		ops:           make(map[OpID]Operation),
		seen:          make(VersionVector),
		gcFloor:       make(VersionVector),
		changes:       make(map[OpID]MemberChangeOp),
		peerReported:  make(map[string]VersionVector),
		reportedPeers: make(map[string]bool),
	}
	return r
}

// ID 返回副本 ID。
func (r *Replica) ID() string { return r.id }

// Snapshot 是用于落后副本全量重同步的快照。
type Snapshot struct {
	Membership Membership
	Seen       VersionVector
	GCFloor    VersionVector
	Ops        []Operation
	Changes    []MemberChangeOp
}

func (r *Replica) snapshotLocked() Snapshot {
	entries := r.logEntriesLocked()
	var ops []Operation
	var changes []MemberChangeOp
	origins := make([]string, 0, len(entries))
	for o := range entries {
		origins = append(origins, o)
	}
	sort.Strings(origins)
	for _, o := range origins {
		for _, e := range entries[o] {
			if e.Op != nil {
				ops = append(ops, *e.Op)
			} else {
				changes = append(changes, *e.Change)
			}
		}
	}
	return Snapshot{
		Membership: r.view.clone(),
		Seen:       r.seen.Clone(),
		GCFloor:    r.gcFloor.Clone(),
		Ops:        ops,
		Changes:    changes,
	}
}

// GetSnapshot 返回当前状态的深拷贝快照。
func (r *Replica) GetSnapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshotLocked()
}

// RestoreFromSnapshot 用对端快照重置自身（用于落后太多无法增量同步的副本）。
// 调用方必须保证该副本已获得集群授权（同步协议返回 SnapshotRequired 时自动进行）。
func (r *Replica) RestoreFromSnapshot(s Snapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.view = s.Membership.clone()
	r.ops = make(map[OpID]Operation, len(s.Ops))
	for _, op := range s.Ops {
		r.ops[op.ID()] = op
	}
	r.seen = s.Seen.Clone()
	r.gcFloor = s.GCFloor.Clone()
	r.changes = make(map[OpID]MemberChangeOp, len(s.Changes))
	for _, x := range s.Changes {
		r.changes[x.ID()] = x
	}
	r.peerReported = make(map[string]VersionVector)
	r.reportedPeers = make(map[string]bool)
	r.recordSelfSeenLocked()
}

func (r *Replica) bumpLocal() (uint64, VersionVector) {
	r.localSeq++
	r.seen[r.id] = r.localSeq
	r.recordSelfSeenLocked()
	return r.localSeq, r.seen.Clone()
}

// recordSelfSeenLocked 把自己的最新 seen 登记为“已自报”：GC 的全员确认
// 集合天然包含本副本，而本副本对自己持有的数据必然“已见过”。
func (r *Replica) recordSelfSeenLocked() {
	if r.view.contains(r.id) {
		cur := r.peerReported[r.id]
		if cur == nil {
			cur = make(VersionVector)
			r.peerReported[r.id] = cur
		}
		cur.Merge(r.seen)
		r.reportedPeers[r.id] = true
	}
}

var errReplicaRetired = errors.New("副本已退役，拒绝本地变更")

// Put 在本副本写入一个键值，返回新操作。
func (r *Replica) Put(key string, value []byte) (Operation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.view.contains(r.id) {
		return Operation{}, fmt.Errorf("causal: 副本 %q %w", r.id, errReplicaRetired)
	}
	seq, ver := r.bumpLocal()
	op := newOp(r.id, seq, key, OpPut, value, ver)
	r.ops[op.ID()] = op
	return op, nil
}

// Delete 在本副本删除一个键（写入墓碑）。
func (r *Replica) Delete(key string) (Operation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.view.contains(r.id) {
		return Operation{}, fmt.Errorf("causal: 副本 %q %w", r.id, errReplicaRetired)
	}
	seq, ver := r.bumpLocal()
	op := newOp(r.id, seq, key, OpDelete, nil, ver)
	r.ops[op.ID()] = op
	return op, nil
}

// Resolve 显式解决冲突：以给定值覆盖，并在版本向量上因果合并所有存活冲突版本，
// 使新版本严格支配被解决的全部版本。
func (r *Replica) Resolve(key string, value []byte) (Operation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.view.contains(r.id) {
		return Operation{}, fmt.Errorf("causal: 副本 %q %w", r.id, errReplicaRetired)
	}
	v, err := r.viewLocked(key)
	if err != nil {
		return Operation{}, err
	}
	for _, e := range v.Entries {
		r.seen.Merge(e.Version)
	}
	seq, ver := r.bumpLocal()
	op := newOp(r.id, seq, key, OpPut, value, ver)
	r.ops[op.ID()] = op
	return op, nil
}

// ResolveDelete 显式解决“值与墓碑并发”的冲突：删除胜出，同时支配所有并发值。
func (r *Replica) ResolveDelete(key string) (Operation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.view.contains(r.id) {
		return Operation{}, fmt.Errorf("causal: 副本 %q %w", r.id, errReplicaRetired)
	}
	v, err := r.viewLocked(key)
	if err != nil {
		return Operation{}, err
	}
	for _, e := range v.Entries {
		r.seen.Merge(e.Version)
	}
	seq, ver := r.bumpLocal()
	op := newOp(r.id, seq, key, OpDelete, nil, ver)
	r.ops[op.ID()] = op
	return op, nil
}
