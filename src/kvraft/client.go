package kvraft

import (
	"6.824/labrpc"
	"crypto/rand"
	"log"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastServer int
	// use nrand()
	ClerkID int64
	// default is 0, increment
	ReqID int
}

type Queue []int

func (q *Queue) Len() int {
	return len(*q)
}

func (q *Queue) Pop() int {
	ret := (*q)[0]
	(*q) = (*q)[1:]
	return ret
}

func (q *Queue) Push(x int) {
	(*q) = append((*q), x)
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
	ck.ClerkID = nrand()
	ck.ReqID = 1
	return ck
}

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
func (ck *Clerk) Get(key string) string {
	DPrintf("C Get: key: \"%v\"\n", key)
	// You will have to modify this function.
	args := &GetArgs{key, ck.ClerkID, ck.ReqID}
	ck.ReqID++
	reply := &GetReply{}
	snum := Queue{ck.lastServer}
	for i := 0; i < len(ck.servers); i++ {
		snum.Push(i)
	}
	for snum.Len() > 0 {
		v := snum.Pop()
		ok := ck.servers[v].Call("KVServer.Get", args, reply)
		if !ok {
			DPrintf("C Get: not ok\n")
			snum.Push(v)
			continue
		}
		if reply.Ok {
			DPrintf("C Get: reply: %v\n", reply)
			ck.lastServer = v
			return reply.Value
		} else {
			snum.Push(v)
			DPrintf("C Get: not leader, continue...\n")
		}
	}
	log.Fatal("fatal")
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	DPrintf("C PutAppend: key: \"%v\", value: \"%v\", op: %v\n", key, value, op)
	args := &PutAppendArgs{key, value, op, ck.ClerkID, ck.ReqID}
	ck.ReqID++
	reply := &PutAppendReply{}
	// reply := make(chan PutAppendReply)
	snum := Queue{ck.lastServer}
	for i := 0; i < len(ck.servers); i++ {
		snum.Push(i)
	}
	for snum.Len() > 0 {
		v := snum.Pop()
		ok := ck.servers[v].Call("KVServer.PutAppend", args, reply)
		if !ok {
			DPrintf("C PutAppend: not ok\n")
			snum.Push(v)
			continue
		}
		if reply.Ok {
			DPrintf("C PutAppend: reply: %v\n", reply)
			ck.lastServer = v
			return
		} else {
			snum.Push(v)
			DPrintf("C PutAppend: not leader, continue...\n")
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
