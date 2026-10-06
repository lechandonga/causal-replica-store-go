package causal

import "testing"

// GC 之后：迟到/重传的旧操作必须被 gcFloor 幂等忽略，不得复活旧值。
func TestBusinessStaleOpsAfterGCIgnored(t *testing.T) {
	t.Log("业务【回收水位：GC 后任何 seq<=gcFloor 的陈旧消息都被忽略】")
	r1, r2, r3 := newCluster3(t)
	p1 := mustPut(t, r1, "k", "v1")
	p2 := mustPut(t, r1, "k", "v2")
	d1 := mustDelete(t, r1, "k")

	// 全员收敛后回收（墓碑支配 p1/p2，确定胜出）。
	SyncToConvergence(r1, r2, 10)
	SyncToConvergence(r1, r3, 10)
	SyncToConvergence(r2, r3, 10)
	SyncToConvergence(r1, r2, 10)
	SyncToConvergence(r1, r3, 10)
	rep := r1.CollectGarbage()
	if len(rep.Reaped) != 3 {
		t.Fatalf("应回收 3 条操作，实际 %d", len(rep.Reaped))
	}
	floor := r1.GCFloor()
	if floor.Get("r1") != 3 {
		t.Fatalf("gcFloor[r1] 应为 3，实际 %d", floor.Get("r1"))
	}
	logStep(t, "v1/v2/墓碑回收后 gcFloor[r1]=3，键不存在")

	// 旧操作以任意顺序、重复重传：全部忽略，不复活。
	stale := []LogEntry{
		{Seq: d1.Seq, Op: &d1},
		{Seq: p2.Seq, Op: &p2},
		{Seq: p1.Seq, Op: &p1},
	}
	n, err := r1.ApplyEntries(stale)
	if err != nil {
		t.Fatalf("陈旧消息应被静默忽略而非报错: %v", err)
	}
	if n != 0 {
		t.Fatalf("陈旧消息不应被应用，实际应用 %d 条", n)
	}
	assertNotFound(t, r1, "k")
	logStep(t, "GC 后旧 PUT/墓碑以乱序+重复方式重传：0 条被应用，旧值未复活")
}
