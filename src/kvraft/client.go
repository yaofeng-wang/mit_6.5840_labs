package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	"sync"

	"6_5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd // always place the last known kv server with the leader raft in the first index
	// You will have to modify this struct.
	ID    int64
	SeqNo int64
	mu    sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ID = nrand()
	return ck
}

func (ck *Clerk) dPrintf(format string, a ...interface{}) (n int, err error) {
	a = append([]interface{}{
		ck.ID,
	}, a...)
	if Debug {
		log.Printf("[Clerk] me=%v "+format, a...)
	}
	return
}

// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	value := ""
	ck.mu.Lock()
	ck.SeqNo++
	args := GetArgs{
		Key:      key,
		ClientID: ck.ID,
		SeqNo:    ck.SeqNo,
	}
	ck.mu.Unlock()
	ck.dPrintf("Get args=%+v", args)

	for i := 0; ; i = (i + 1) % len(ck.servers) {
		reply := GetReply{}
		if ok := ck.servers[i].Call("KVServer.Get", &args, &reply); ok {
			ck.dPrintf("Get args=%+v reply=%+v", args, reply)
			if reply.Err == ErrWrongLeader {
				continue
			}

			if i != 0 {
				ck.mu.Lock()
				ck.servers[0], ck.servers[i] = ck.servers[i], ck.servers[0]
				ck.mu.Unlock()
			}
			value = reply.Value
			break
		}
	}
	return value
}

// PutAppend shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	ck.SeqNo++
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientID: ck.ID,
		SeqNo:    ck.SeqNo,
	}
	ck.mu.Unlock()
	ck.dPrintf("PutAppend args=%+v", args)

	for i := 0; ; i = (i + 1) % len(ck.servers) {
		reply := PutAppendReply{}
		if ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply); ok {
			ck.dPrintf("PutAppend args=%+v reply=%+v", args, reply)
			if reply.Err == ErrWrongLeader {
				continue
			}

			if i != 0 {
				ck.mu.Lock()
				ck.servers[0], ck.servers[i] = ck.servers[i], ck.servers[0]
				ck.mu.Unlock()
			}
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
