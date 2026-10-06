package causal

import "time"

// OpKind 是操作类型。
type OpKind int

const (
	// OpPut 写入一个值。
	OpPut OpKind = iota
	// OpDelete 删除某个键（产生墓碑）。
	OpDelete
)

func (k OpKind) String() string {
	if k == OpDelete {
		return "DEL"
	}
	return "PUT"
}

// Operation 是因果复制的最小不可变单元。
//
// 每个操作由 (Origin, Seq) 全局唯一标识；Version 是操作发生时刻的完整
// 向量时钟，用于因果比较、冲突检测与 GC 安全判定。
type Operation struct {
	Origin  string
	Seq     uint64
	Key     string
	Kind    OpKind
	Value   []byte
	Version VersionVector
	// WallTime 仅用于稳定排序与排障，不参与因果语义。
	WallTime int64
}

func newOp(origin string, seq uint64, key string, kind OpKind, value []byte, ver VersionVector) Operation {
	return Operation{
		Origin:   origin,
		Seq:      seq,
		Key:      key,
		Kind:     kind,
		Value:    append([]byte(nil), value...),
		Version:  ver.Clone(),
		WallTime: time.Now().UnixNano(),
	}
}

// OpID 是操作的全局唯一标识。
type OpID struct {
	Origin string
	Seq    uint64
}

func (o Operation) ID() OpID {
	return OpID{Origin: o.Origin, Seq: o.Seq}
}

// Membership 是集群成员视图。Epoch 单调递增，成员列表随退役事件变化。
type Membership struct {
	Epoch   uint64
	Members []string
}

func (m Membership) clone() Membership {
	return Membership{Epoch: m.Epoch, Members: append([]string(nil), m.Members...)}
}

func (m Membership) contains(id string) bool {
	for _, x := range m.Members {
		if x == id {
			return true
		}
	}
	return false
}

// ChangeKind 是成员变更类型。
type ChangeKind int

const (
	// ChangeAdd 加入一个成员。
	ChangeAdd ChangeKind = iota + 1
	// ChangeRetire 永久退役一个成员。
	ChangeRetire
)

// MemberChangeOp 是成员变更控制事件，与数据操作共享 (origin, seq) 序号空间，
// 保证它与同时期的数据操作有确定的因果先后。
type MemberChangeOp struct {
	Origin   string
	Seq      uint64
	Kind     ChangeKind
	TargetID string
	Epoch    uint64
	Members  []string
	Version  VersionVector
	WallTime int64
}

// IsRetire 报告该变更是否为退役。
func (m MemberChangeOp) IsRetire() bool { return m.Kind == ChangeRetire }

func (m MemberChangeOp) ID() OpID {
	return OpID{Origin: m.Origin, Seq: m.Seq}
}

// LogEntry 是统一日志流中的一个事件：数据操作或成员变更。
// 同步协议只在 LogEntry 上工作，从而用同一套幂等/乱序/重传逻辑
// 处理数据与成员变更。
type LogEntry struct {
	Seq    uint64
	Op     *Operation
	Change *MemberChangeOp
}

// VersionOf 返回该日志事件携带的向量时钟。
func (e LogEntry) VersionOf() VersionVector {
	if e.Op != nil {
		return e.Op.Version
	}
	return e.Change.Version
}
