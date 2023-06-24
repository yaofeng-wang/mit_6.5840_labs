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

const Debug = false

func (kv *KVServer) dPrintf(format string, a ...interface{}) (n int, err error) {
	a = append([]interface{}{
		time.Since(kv.start).Milliseconds(),
		kv.me,
	}, a...)
	if Debug {
		log.Printf("%v [KVServer] me=%v "+format, a...)
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
	OpType   OpType
	Key      string
	Value    string
	ClientID int64
	SeqNo    int64
	Term     int
}

type DuplicateTableEntry struct {
	SeqNo int64
	Term  int
	Value string
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
	duplicateTable    map[int64]DuplicateTableEntry

	start time.Time
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	term, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// request delayed for a long time
	// client is not waiting for the response, just ignore
	// NOTE: Each client will only have 1 ongoing request.
	kv.mu.Lock()
	if args.SeqNo < kv.duplicateTable[args.ClientID].SeqNo {
		kv.mu.Unlock()
		return
	}
	if args.SeqNo == kv.duplicateTable[args.ClientID].SeqNo {
		reply.Value = kv.duplicateTable[args.ClientID].Value
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// submit request with higher SegNo and request with same SegNo

	kv.dPrintf("Get args=%+v", args)
	op := Op{
		OpType:   OpTypeGet,
		Key:      args.Key,
		ClientID: args.ClientID,
		SeqNo:    args.SeqNo,
		Term:     term,
	}

	logIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.dPrintf("Get args=%+v logIndex=%v term=%v", args, logIndex, term)
	logIndexCommittedCh := make(chan Op)
	kv.mu.Lock()
	kv.pendingLogIndices[logIndex] = logIndexCommittedCh
	kv.mu.Unlock()

	for {
		select {
		// blocks until the logIndex is committed
		case receivedOp := <-logIndexCommittedCh:
			kv.mu.Lock()
			delete(kv.pendingLogIndices, logIndex)
			kv.mu.Unlock()
			if receivedOp.Term == op.Term {
				reply.Value = receivedOp.Value
				reply.Err = OK
			} else {
				reply.Err = ErrWrongLeader
			}
			kv.dPrintf("Get end reply=%+v", reply)
			return
		case <-time.After(100 * time.Millisecond):
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				kv.mu.Lock()
				delete(kv.pendingLogIndices, logIndex)
				kv.mu.Unlock()
				kv.dPrintf("Not current leader reply=%+v", reply)
				reply.Err = ErrWrongLeader
				return
			}
		}
	}

	kv.dPrintf("Get end")
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	term, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// request delayed for a long time
	// client is not waiting for the response, just ignore
	// NOTE: Each client will only have 1 ongoing request.
	kv.mu.Lock()
	if args.SeqNo < kv.duplicateTable[args.ClientID].SeqNo {
		kv.mu.Unlock()
		return
	}

	if args.SeqNo == kv.duplicateTable[args.ClientID].SeqNo {
		reply.Err = OK
		kv.mu.Unlock()
		kv.dPrintf("PutAppend args.SeqNo == kv.duplicateTable[args.ClientID].SeqNo end args.SeqNo=%v reply=%+v", args.SeqNo, reply)
		return
	}
	kv.mu.Unlock()

	kv.dPrintf("PutAppend args=%+v", args)
	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientID,
		SeqNo:    args.SeqNo,
		Term:     term,
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

	kv.dPrintf("PutAppend args=%+v logIndex=%v term=%v", args, logIndex, term)

	logIndexCommittedCh := make(chan Op)
	kv.mu.Lock()
	kv.pendingLogIndices[logIndex] = logIndexCommittedCh
	kv.mu.Unlock()

	for {
		select {
		case receivedOp := <-logIndexCommittedCh:
			// blocks until the logIndex is committed
			kv.mu.Lock()
			delete(kv.pendingLogIndices, logIndex)
			kv.mu.Unlock()
			kv.dPrintf("receivedOp.Term=%v op.Term=%v", receivedOp.Term, op.Term)
			if receivedOp.Term == op.Term {
				reply.Err = OK
			} else {
				reply.Err = ErrWrongLeader
			}
			kv.dPrintf("PutAppend end reply=%+v", reply)
			return
		case <-time.After(100 * time.Millisecond):

			_, isLeader := kv.rf.GetState()
			if !isLeader {
				kv.mu.Lock()
				delete(kv.pendingLogIndices, logIndex)
				kv.mu.Unlock()
				reply.Err = ErrWrongLeader
				kv.dPrintf("Not current leader reply=%+v", reply)
				return
			}
		}
	}

}

func (kv *KVServer) readApplyCh() {
	for msg := range kv.applyCh {
		logIndex := msg.CommandIndex
		op := msg.Command.(Op)
		kv.dPrintf("readApplyCh  msg=%+v", msg)
		kv.mu.Lock()

		// logs may have the same SeqNo, submitted by separate leaders
		if op.SeqNo <= kv.duplicateTable[op.ClientID].SeqNo {
			kv.dPrintf("readApplyCh op.SeqNo=%v kv.duplicateTable[op.ClientID].SeqNo=%v", op.SeqNo, kv.duplicateTable[op.ClientID].SeqNo)
			if _, ok := kv.pendingLogIndices[logIndex]; ok {
				close(kv.pendingLogIndices[logIndex])
				delete(kv.pendingLogIndices, logIndex)
			}
			kv.mu.Unlock()
			continue
		}
		switch op.OpType {
		case OpTypePut:
			kv.keyValues[op.Key] = op.Value
		case OpTypeGet:
			op.Value = kv.keyValues[op.Key]
		case OpTypeAppend:
			op.Value = kv.keyValues[op.Key] + op.Value
			kv.keyValues[op.Key] = op.Value
		}
		if pendingCh, ok := kv.pendingLogIndices[logIndex]; ok {
			kv.dPrintf("readApplyCh op=%+v", op)
			delete(kv.pendingLogIndices, logIndex)
			pendingCh <- op
		}

		kv.dPrintf("readApplyCh pendingCh logIndex=%v msg=%+v", logIndex, msg)
		kv.duplicateTable[op.ClientID] = DuplicateTableEntry{
			SeqNo: op.SeqNo,
			Term:  op.Term,
			Value: op.Value,
		}
		kv.mu.Unlock()

	}
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
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, v := range kv.pendingLogIndices {
		close(v)
	}
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
	kv.duplicateTable = make(map[int64]DuplicateTableEntry)

	// You may need initialization code here.
	go kv.readApplyCh()

	if len(kv.rf.Logs) > 0 {
		kv.dPrintf("started len(kv.rf.Logs)=%v lastCommand=%+v", len(kv.rf.Logs), kv.rf.Logs[len(kv.rf.Logs)-1].Command)
	} else {
		kv.dPrintf("started len(kv.rf.Logs)=%v", len(kv.rf.Logs))
	}

	return kv
}
