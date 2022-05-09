package kvraft

import (
	"crypto/rand"
	"math/big"

	"sync/atomic"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ClientID  int
	RequestID int32
	LeaderID  int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) GetRequestID() int   { return int(atomic.LoadInt32(&ck.RequestID)) }
func (ck *Clerk) IncrementRequestID() { atomic.AddInt32(&ck.RequestID, 1) }

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ClientID = int(nrand())
	ck.RequestID = 0
	ck.LeaderID = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key:       key,
		ClientID:  ck.ClientID,
		RequestID: ck.GetRequestID(),
	}
	ck.IncrementRequestID()

	for leaderID := int(ck.LeaderID); ; leaderID = (leaderID + 1) % len(ck.servers) {
		// DPrintf("keep asking %d", leaderID)
		leader := ck.servers[leaderID]
		reply := GetReply{}

		args.SentTo = leaderID

		ok := leader.Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err != ErrWrongLeader {
				ck.LeaderID = int32(leaderID)
			}
			if reply.Err == OK {
				return reply.Value
			}
			if reply.Err == ErrNoKey {
				return ""
			}
		} else {
			DPrintf("Get call failed. Trying again")
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	// You will have to modify this function.
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientID:  ck.ClientID,
		RequestID: ck.GetRequestID(),
	}
	ck.IncrementRequestID()

	// DPrintf("putappend args %v", args)

	for leaderID := int(ck.LeaderID); ; leaderID = (leaderID + 1) % len(ck.servers) {
		leader := ck.servers[leaderID]
		reply := GetReply{}
		args.SentTo = leaderID
		ok := leader.Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err != ErrWrongLeader {
				ck.LeaderID = int32(leaderID)
			}
			if reply.Err == OK {
				return
			}
		} else {
			DPrintf("PutAppend call failed. Trying again")
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
