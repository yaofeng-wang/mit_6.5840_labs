package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6_5840/labgob"
	"6_5840/labrpc"
	"6_5840/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType int

const (
	OpTypeGet OpType = iota
	OpTypePut
	OpTypeAppend
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType OpType
	Key    string
	Value  string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	keyValues         map[string]string
	pendingLogIndices map[int]chan Op

	start time.Time
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		OpType: OpTypeGet,
		Key:    args.Key,
	}

	logIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("%v [server] me=%v Get key=%v logIndex=%v",
		time.Since(kv.start).Milliseconds(), kv.me, op.Key, logIndex)
	logIndexCommittedCh := make(chan Op)
	kv.mu.Lock()
	kv.pendingLogIndices[logIndex] = logIndexCommittedCh
	kv.mu.Unlock()

	// blocks until the logIndex is committed
	resp := <-logIndexCommittedCh
	kv.mu.Lock()
	delete(kv.pendingLogIndices, logIndex)
	kv.mu.Unlock()
	reply.Value = resp.Value
	reply.Err = OK
}

func (kv *KVServer) readApplyCh() {
	for msg := range kv.applyCh {
		logIndex := msg.CommandIndex
		op := msg.Command.(Op)
		DPrintf("%v [server] me=%v readApplyCh msg=%v",
			time.Since(kv.start).Milliseconds(), kv.me, msg)
		kv.mu.Lock()
		switch op.OpType {
		case OpTypePut:
			kv.keyValues[op.Key] = op.Value
		case OpTypeGet:
			op.Value = kv.keyValues[op.Key]
		case OpTypeAppend:
			op.Value = kv.keyValues[op.Key] + op.Value
			kv.keyValues[op.Key] = op.Value
		}
		pendingCh := kv.pendingLogIndices[logIndex]
		kv.mu.Unlock()

		if pendingCh != nil {
			pendingCh <- op
		}

	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Key:   args.Key,
		Value: args.Value,
	}
	switch args.Op {
	case "Put":
		op.OpType = OpTypePut
	case "Append":
		op.OpType = OpTypeAppend
	}

	logIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("%v [server] me=%v PutAppend args.Op=%v key=%v value=%v logIndex=%v",
		time.Since(kv.start).Milliseconds(), kv.me, args.Op, op.Key, op.Value, logIndex)

	logIndexCommittedCh := make(chan Op)
	kv.mu.Lock()
	kv.pendingLogIndices[logIndex] = logIndexCommittedCh
	kv.mu.Unlock()

	// blocks until the logIndex is committed
	<-logIndexCommittedCh
	kv.mu.Lock()
	delete(kv.pendingLogIndices, logIndex)
	kv.mu.Unlock()
	reply.Err = OK
}

// Kill the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.applyCh)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, start time.Time) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.start = start
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.MakeRaft(servers, me, persister, kv.applyCh, kv.start)
	kv.keyValues = make(map[string]string)
	kv.pendingLogIndices = make(map[int]chan Op)
	// You may need initialization code here.
	go kv.readApplyCh()

	DPrintf("%v me=%v started", time.Since(kv.start).Milliseconds(), me)
	return kv
}
