package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OpGet int = iota
	OpPut
	OpAppend
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type    int // OpGet, OpPut, OpAppend
	Key     string
	Val     string
	ClerkID int64
	ReqID   int
	Done    chan string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database map[string]string
	dNum     int

	// key: ClientID, val: ReqID
	// clientResults       map[int64]clientReqResult
	clientReqResult map[int64]int
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		DPrintf("S%v applier: receive %v\n", kv.dNum, applyMsg)
		if applyMsg.CommandValid {
			DPrintf("S%v applier: msg valid\n", kv.dNum)
			// NOTE:
			// 1. 如果创建此op的peer和这个applier的peer相同,op.Done有效
			// 2. 如果创建此op的peer和这个applier的peer不相同,op.Done为nil channel
			op := applyMsg.Command.(Op)
			kv.mu.Lock()
			if kv.clientReqResult[op.ClerkID] < op.ReqID {
				kv.clientReqResult[op.ClerkID] = op.ReqID
				if op.Type == OpPut {
					kv.database[op.Key] = op.Val
				} else if op.Type == OpAppend {
					kv.database[op.Key] += op.Val
				}
				DPrintf("S%v database: %v\n", kv.dNum, kv.database)
			}
			// wake up Done channel
			go func(done chan string, val string) {
				if done != nil {
					// 应该由发送方负责关闭channel
					select {
					case done <- val:
					// 如果接受方没有接受done信号,就认为接受方已经不再等待改channel,此goroutine应该return
					case <-time.After(NetWorkTimeout):
					}
				}
			}(op.Done, kv.database[op.Key])
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 因为raft库的字段不保留给上层KVServer，所以KVServer使用Start()方法和raft库进行交互
	DPrintf("S%v Get: args: %v\n", kv.dNum, args)
	done := make(chan string)
	op := Op{
		Type:    OpGet,
		Key:     args.Key,
		ClerkID: args.ClerkID,
		ReqID:   args.ReqID,
		Done:    done,
	}

	idx, term, isLeader := kv.rf.Start(op)
	DPrintf("S%v Get: %v, %v, %v\n", kv.dNum, idx, term, isLeader)
	if !isLeader {
		return
	}
	select {
	case wakeMsg, ok := <-done:
		DPrintf("S%v Get: ok:%v\n", kv.dNum, ok)
		DPrintf("S%v Get: wakeMsg: %v, ok: %v\n", kv.dNum, wakeMsg, ok)
		reply.Ok = true
		reply.Value = wakeMsg
	case <-time.After(NetWorkTimeout):
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("S%v PutAppend: args: %v\n", kv.dNum, args)
	done := make(chan string)
	var op Op
	if args.Op == "Put" {
		op = Op{
			Type:    OpPut,
			Key:     args.Key,
			Val:     args.Value,
			ClerkID: args.ClerkID,
			ReqID:   args.ReqID,
			Done:    done,
		}
	} else if args.Op == "Append" {
		op = Op{
			Type:    OpAppend,
			Key:     args.Key,
			Val:     args.Value,
			ClerkID: args.ClerkID,
			ReqID:   args.ReqID,
			Done:    done,
		}
	}

	// NOTE:
	// 1. 当前peer不是Leader,第一次接受到某个clerk a的PutAppend
	// 2. 当前peer的kv.clientReqResult=map{a: 1}
	// 3. 很短的时间内,因为还没有选出leader,clerk a很快重发PutAppend,当前peer再次接受到clerk a的PutAppend
	// 4. 此时kv.clientReqResult=map{a: 1},所以直接构造了Append(""),导致返回值错误
	// 5. 所以,应该把检查重复req的逻辑放在applier()中,在更改kv.database前检查是否重复req
	DPrintf("S%v PutAppend: op: %v\n", kv.dNum, op)
	idx, term, isLeader := kv.rf.Start(op)
	DPrintf("S%v PutAppend: %v, %v, %v\n", kv.dNum, idx, term, isLeader)
	if !isLeader {
		return
	}
	select {
	case wakeMsg, ok := <-done:
		DPrintf("S%v PutAppend: wakeMsg: %v, ok: %v\n", kv.dNum, wakeMsg, ok)
		DPrintf("S%v database: %v\n", kv.dNum, kv.database)
		reply.Ok = true
	case <-time.After(NetWorkTimeout):
		return
	}
}

// the tester calls Kill() when a KVServer instance won't
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
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.dNum = kv.me
	kv.database = make(map[string]string)

	// You may need initialization code here.
	// kv.clientResults = map[int64]clientReqResult{}
	kv.clientReqResult = map[int64]int{}
	go kv.applier()

	return kv
}
