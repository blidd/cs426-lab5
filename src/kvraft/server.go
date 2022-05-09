package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string
	ClientID  int
	RequestID int
	Error     Err
	SentTo    int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store   map[string]string
	applied map[int]chan Op // command index -> value to returns
	last    map[int]int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Operation: "Get",
		Key:       args.Key,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
		SentTo:    args.SentTo,
	}

	DPrintf("GET [op: %s, key: %s, value: %s, client: %d, request: %d, sentto: %d]", op.Operation, op.Key, op.Value, op.ClientID, op.RequestID, op.SentTo)

	// send a Get operation to your Raft peer
	idx, _, isLeader := kv.rf.Start(op)
	if !isLeader { // if the peer is not the leader, return wrong leader err
		reply.Value = ""
		reply.Err = ErrWrongLeader
		DPrintf("GET wrong leader")
		return
	}

	kv.mu.Lock()
	// make channel for RPC handler to listen on for applied updates
	if _, ok := kv.applied[idx]; !ok {
		kv.applied[idx] = make(chan Op, 1) // buffer channel
	}
	kv.mu.Unlock()

	// wait here for an update. If no updates before timeout, return error
	// and tell the client to check who is leader and resend the request
	select {
	case appliedOp := <-kv.applied[idx]:
		// make sure that this command is the same one that the client requested
		// the reply value for
		if op.ClientID == appliedOp.ClientID && op.RequestID == appliedOp.RequestID {
			reply.Value = appliedOp.Value
			reply.Err = appliedOp.Error
			DPrintf("GET reply value: %s, error: %s", reply.Value, reply.Err)
			return
		} else {
			// if a different command was applied at the same index, then this
			// peer knows that it is no longer the leader.
			reply.Value = ""
			reply.Err = ErrWrongLeader
			return
		}
	case <-time.After(500 * time.Millisecond):
		reply.Value = ""
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	op := Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
		SentTo:    args.SentTo,
	}

	DPrintf("PUTAPPEND [op: %s, key: %s, value: %s, client: %d, request: %d, sentto: %d]", op.Operation, op.Key, op.Value, op.ClientID, op.RequestID, op.SentTo)

	// send operation to your Raft peer
	idx, _, isLeader := kv.rf.Start(op)
	if !isLeader { // if the peer is not the leader, return wrong leader err
		reply.Err = ErrWrongLeader
		DPrintf("PUTAPP wrong leader")
		return
	}

	// make channel for RPC handler to listen on for applied updates
	kv.mu.Lock()
	if _, ok := kv.applied[idx]; !ok {
		kv.applied[idx] = make(chan Op, 1) // buffer channel
	}
	kv.mu.Unlock()

	select {
	case appliedOp := <-kv.applied[idx]:
		// make sure that this command is the same one that the client requested
		// the reply value for
		if op.ClientID == appliedOp.ClientID && op.RequestID == appliedOp.RequestID {
			reply.Err = OK
			DPrintf("PUTAPP reply error: %s", reply.Err)

		} else {
			// if a different command was applied at the same index, then this
			// peer knows that it is no longer the leader.
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) duplicated(op Op) bool {
	lastReqID, ok := kv.last[op.ClientID]
	if !ok {
		return false
	}
	if op.RequestID > lastReqID {
		return false
	}
	return true
}

func (kv *KVServer) apply() {
	for {
		applyMsg := <-kv.applyCh

		kv.mu.Lock()
		op := applyMsg.Command.(Op)
		DPrintf("APPLYCH [op: %s, key: %s, value: %s, client: %d, request: %d, sentto: %d", op.Operation, op.Key, op.Value, op.ClientID, op.RequestID, op.SentTo)

		switch op.Operation {
		case "Get":
			if val, ok := kv.store[op.Key]; ok {
				op.Value = val
				op.Error = OK
			} else {
				op.Value = ""
				op.Error = ErrNoKey
			}
			if !kv.duplicated(op) {
				kv.last[op.ClientID] = op.RequestID // only update requestID if it isnt duplicated
			} else {
				DPrintf("duplicate command %s. client: %d, request: %d, lastID: %d", op.Operation, op.ClientID, op.RequestID, kv.last[op.ClientID])
			}
		case "Put":
			if !kv.duplicated(op) {
				kv.store[op.Key] = op.Value
				op.Error = OK
				kv.last[op.ClientID] = op.RequestID
			} else {
				DPrintf("duplicate command %s. client: %d, request: %d, lastID: %d", op.Operation, op.ClientID, op.RequestID, kv.last[op.ClientID])
			}
		case "Append":
			if !kv.duplicated(op) {
				kv.store[op.Key] = kv.store[op.Key] + op.Value
				op.Error = OK
				kv.last[op.ClientID] = op.RequestID
			} else {
				DPrintf("duplicate command %s. client: %d, request: %d, lastID: %d", op.Operation, op.ClientID, op.RequestID, kv.last[op.ClientID])
			}
		}

		// if !ok || (op.RequestID > lastReqID) { // check that the command is not a duplicate
		// 	// execute operation on kv store
		// 	switch op.Operation {
		// 	case "Get":
		// 		if val, ok := kv.store[op.Key]; ok {
		// 			op.Value = val
		// 			op.Error = OK
		// 		} else {
		// 			op.Error = ErrNoKey
		// 		}
		// 	case "Put":
		// 		kv.store[op.Key] = op.Value
		// 		op.Error = OK
		// 	case "Append":
		// 		kv.store[op.Key] = kv.store[op.Key] + op.Value
		// 		op.Error = OK
		// 	default:
		// 		fmt.Printf("invalid operation '%v'\n", op.Operation)
		// 	}
		// 	kv.last[op.ClientID] = op.RequestID // only update requestID if it isnt duplicated
		// } else {
		// 	DPrintf("duplicate command %s. client: %d, request: %d, lastID: %d", op.Operation, op.ClientID, op.RequestID, lastReqID)
		// 	op.Value =
		// 	op.Error = OK
		// }

		// if kv.applied channel for this command doesn't exist yet, it might be the case
		// that the command was committed, sent on the applyCh, and applied to the KV store
		// before the RPC handler could set up a channel to receive the applied op. So
		// in case it doesn't exist yet, we set one up.

		// The problem is if the command wasn't submitted by this server's RPC handler,
		// then we are allocating memory that will never be recovered because there are no
		// RPC handlers waiting for a response for that command index.
		// if _, ok := kv.applied[applyMsg.CommandIndex]; !ok {
		// 	kv.applied[applyMsg.CommandIndex] = make(chan Op, 1)
		// }

		kv.applied[applyMsg.CommandIndex] <- op
		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.applied = make(map[int]chan Op)
	kv.last = make(map[int]int)

	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.apply()

	return kv
}
