package causal

import (
	"math/rand"
	"sync"
)

// FaultProfile 描述一条链路上的故障注入策略。所有策略都是确定性的：
// 给定同一个随机种子，多次运行结果完全一致。
type FaultProfile struct {
	// Seed 为随机故障源的种子。
	Seed int64
	// DropRate 丢弃概率 [0,1)：消息直接消失，由对端下一轮重传来恢复。
	DropRate float64
	// DupRate 重复概率：消息额外再投递一份。
	DupRate float64
	// ReorderRate 乱序概率：消息被延迟一轮投递（模拟乱序到达）。
	ReorderRate float64
	// BreakEvery > 0 时，每传输 BreakEvery 条消息强制断连一次（链路关闭，
	// 需要显式 Reconnect）。0 表示不断连。
	BreakEvery int
}

type envelope struct {
	from, to string
	payload  SyncMessage
}

type linkState struct {
	mu      sync.Mutex
	open    bool
	rng     *rand.Rand
	profile FaultProfile
	sent    int
	delayed []envelope
}

// Network 是进程内的消息通道模拟网络。
// 每个副本有一个收件箱 channel；Send 在入箱前按链路 FaultProfile 注入故障。
type Network struct {
	mu sync.Mutex

	boxes map[string]chan SyncMessage
	links map[string]map[string]*linkState

	// 审计统计：让测试可以直接看到“丢了多少、重了多少、断了几次”。
	Stats map[string]*LinkStats
}

// LinkStats 是单条有向链路的传输统计。
type LinkStats struct {
	Sent       int
	Delivered  int
	Dropped    int
	Duplicated int
	Reordered  int
	Breaks     int
}

// NewNetwork 创建模拟网络并注册副本收件箱。
func NewNetwork(replicaIDs []string, bufSize int) *Network {
	n := &Network{
		boxes: make(map[string]chan SyncMessage),
		links: make(map[string]map[string]*linkState),
		Stats: make(map[string]*LinkStats),
	}
	for _, id := range replicaIDs {
		n.boxes[id] = make(chan SyncMessage, bufSize)
	}
	return n
}

// SetFault 为 from->to 的有向链路配置故障注入。
func (n *Network) SetFault(from, to string, p FaultProfile) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.links[from] == nil {
		n.links[from] = make(map[string]*linkState)
	}
	n.links[from][to] = &linkState{
		open:    true,
		rng:     rand.New(rand.NewSource(p.Seed)),
		profile: p,
	}
	key := from + "->" + to
	if n.Stats[key] == nil {
		n.Stats[key] = &LinkStats{}
	}
}

// Disconnect 断开 from<->to 双向链路（后续消息计入丢弃）。
func (n *Network) Disconnect(a, b string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	setOpen := func(from, to string, open bool) {
		if ls := n.links[from]; ls != nil {
			if l := ls[to]; l != nil {
				l.mu.Lock()
				l.open = open
				l.mu.Unlock()
			}
		}
	}
	setOpen(a, b, false)
	setOpen(b, a, false)
}

// Reconnect 恢复 from<->to 双向链路，并投递之前被延迟（乱序）的消息。
func (n *Network) Reconnect(a, b string) {
	n.mu.Lock()
	setOpen := func(from, to string, open bool) []envelope {
		if ls := n.links[from]; ls != nil {
			if l := ls[to]; l != nil {
				l.mu.Lock()
				l.open = open
				d := l.delayed
				l.delayed = nil
				l.mu.Unlock()
				return d
			}
		}
		return nil
	}
	da := setOpen(a, b, true)
	db := setOpen(b, a, true)
	boxA, boxB := n.boxes[a], n.boxes[b]
	n.mu.Unlock()

	for _, e := range da {
		deliver(boxB, e.payload)
	}
	for _, e := range db {
		deliver(boxA, e.payload)
	}
}

func deliver(box chan SyncMessage, m SyncMessage) {
	select {
	case box <- m:
	default: // 缓冲满视为丢弃，下一轮重传恢复
	}
}

// Send 从 from 向 to 发送一条消息，按链路配置注入故障。
// 返回 false 表示链路断开或消息被丢弃（调用方可据此触发重连/重传）。
func (n *Network) Send(from, to string, m SyncMessage) bool {
	n.mu.Lock()
	box := n.boxes[to]
	var l *linkState
	if n.links[from] != nil {
		l = n.links[from][to]
	}
	key := from + "->" + to
	st := n.Stats[key]
	if st == nil {
		st = &LinkStats{}
		n.Stats[key] = st
	}
	n.mu.Unlock()

	if box == nil {
		return false
	}
	if l == nil {
		st.Sent++
		st.Delivered++
		deliver(box, m)
		return true
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	st.Sent++

	if !l.open {
		st.Dropped++
		return false
	}

	l.sent++
	if l.profile.BreakEvery > 0 && l.sent%l.profile.BreakEvery == 0 {
		l.open = false
		st.Breaks++
		st.Dropped++
		return false
	}

	rng := l.rng
	if rng.Float64() < l.profile.DropRate {
		st.Dropped++
		return false
	}

	// 乱序：先扣下，下一条消息触发时或重连时再放。
	if rng.Float64() < l.profile.ReorderRate {
		st.Reordered++
		l.delayed = append(l.delayed, envelope{from: from, to: to, payload: m})
		// 同时把更早的延迟消息补发出去，制造实际的乱序。
		if len(l.delayed) > 1 {
			old := l.delayed[0]
			l.delayed = l.delayed[1:]
			st.Delivered++
			deliver(box, old.payload)
		}
		return true
	}

	st.Delivered++
	deliver(box, m)

	if rng.Float64() < l.profile.DupRate {
		st.Duplicated++
		deliver(box, m)
	}

	// 若还有积压的延迟消息，本次顺带补发一条。
	if len(l.delayed) > 0 {
		old := l.delayed[0]
		l.delayed = l.delayed[1:]
		st.Delivered++
		deliver(box, old.payload)
	}
	return true
}

// Recv 阻塞接收发给 id 的一条消息。
func (n *Network) Recv(id string) (SyncMessage, bool) {
	box := n.boxes[id]
	if box == nil {
		return SyncMessage{}, false
	}
	m, ok := <-box
	return m, ok
}

// Pending 返回某副本收件箱中当前待处理消息数。
func (n *Network) Pending(id string) int {
	n.mu.Lock()
	box := n.boxes[id]
	n.mu.Unlock()
	if box == nil {
		return 0
	}
	return len(box)
}

// DrainPending 非阻塞地取出收件箱中的全部待处理消息。
func (n *Network) DrainPending(id string) []SyncMessage {
	out := make([]SyncMessage, 0, n.Pending(id))
	for {
		select {
		case m := <-n.boxes[id]:
			out = append(out, m)
		default:
			return out
		}
	}
}
