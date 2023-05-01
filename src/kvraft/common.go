package kvraft

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"

	NetWorkTimeout = 200 * time.Millisecond
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkID int64
	ReqID   int
}

type PutAppendReply struct {
	Ok  bool
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkID int64
	ReqID   int
}

type GetReply struct {
	Ok    bool
	Err   Err
	Value string
}
