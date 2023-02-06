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
	// You will have to modify this function.
	args := &GetArgs{key}
	reply := &GetReply{}
	snum := []int{ck.lastServer}
	for i := 0; i < len(ck.servers); i++ {
		snum = append(snum, i)
	}
	for _, v := range snum {
		ok := ck.servers[v].Call("KVServer.Get", args, reply)
		if !ok {
			log.Fatalf("C Get\n")
		}
		if reply.Ok {
			DPrintf("C Get: reply: %v\n", reply)
			ck.lastServer = v
			return reply.Value
		}
		// return ""
	}
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
	args := &PutAppendArgs{key, value, op}
	reply := &PutAppendReply{}
	snum := []int{ck.lastServer}
	for i := 0; i < len(ck.servers); i++ {
		snum = append(snum, i)
	}
	for _, v := range snum {
		ok := ck.servers[v].Call("KVServer.PutAppend", args, reply)
		if !ok {
			log.Fatalf("C PutAppend\n")
		}
		if reply.Ok {
			DPrintf("C PutAppend: reply: %v\n", reply)
			ck.lastServer = v
			return
		} else {
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
