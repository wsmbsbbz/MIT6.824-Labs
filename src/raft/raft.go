package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"math/rand"

	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	// Raft.state
	Leader = 1
	// Candidate = 2
	Follower = 3
	// timeout
	MinTick = 200 * time.Millisecond
	TickInterval = 300 * time.Millisecond
	HeartbeatInterval = 100 * time.Millisecond
)

type logEntry struct {
	// NOTE: first index is 1
	Index int
	Term int
	// TODO: command
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers:
	currentTerm int
	// 在当前term未投票时,votedFor = -1
	votedFor int
	// NOTE: temporary
	log []logEntry
	// volatile state on all servers:
	commitIndex int
	lastApplied int
	// volatile state on leaders:
	nextIndex []int
	matchIndex []int

	// for leader election:
	state int // leader, candidate, follower
	votes int

	// heartbeatInterval time.Ticker
	// NOTE: 每次收到AppendEntries RPC,会重置timeout
	heartbeat <- chan time.Time
}

func randomDuration() time.Duration {
	rand.Seed(time.Now().UnixNano())
	ret := MinTick + time.Duration(rand.Intn(int(TickInterval)))
	// Debugf("randomDuration: ret: %v\n", ret)
	return ret
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// return term, isleader
	// Your code here (2A).
	// WARR: 需要lock吗?
	Debugf("GetState: %v\n", rf)
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	// 所有CandidateId应该大于0
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debugf("RequestVote-Start: %v\n", rf)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if args.Term > rf.currentTerm {
		// NOTE: 接受任何RPC请求时,发现currentTerm < args.Term,需要更新currentTerm并转换为follower
		rf.coverTerm(args.Term)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		if rf.votedFor < 0 {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	}
	reply.Term = rf.currentTerm
	Debugf("RequestVote-End: %v\n", rf)
	// TODO: 检查args.LastLogIndex和args.LastLogTerm是否符合paper中的要求
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// heartbeat
	// WARR: 此时有race
	go func() {
		Debugf("Chan waiting\n")
		rf.heartbeat = time.After(HeartbeatInterval)
		Debugf("Chan emitted\n")
	}()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debugf("AppendEntries-Start: %v", rf)
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else if args.Term > rf.currentTerm {
		rf.coverTerm(args.Term)
		reply.Success = true
	} else {
		reply.Success = true
	}
	reply.Term = rf.currentTerm
	Debugf("AppendEntries-End: %v", rf)
	// TODO: Receiver implementation 2-5
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rand.Seed(time.Now().Unix())
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		if rf.state == Leader {
			Debugf("%v is leader\n", rf.me)
			// NOTE: 每隔heartbeatInterval,就发起一轮heartbeat
			args := &AppendEntriesArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				// PrevLogIndex: ,
				// PrevLogTerm: ,
				// Entries: ,
				// LeaderCommit: ,
			}
			for i := range rf.peers {
				reply := &AppendEntriesReply{}
				go func(i int) {
					rf.sendAppendEntries(i, args, reply)
					// TODO: 处理log
					if reply.Term > rf.currentTerm {
						rf.mu.Lock()
						rf.coverTerm(reply.Term)
						rf.mu.Unlock()
					}
				}(i)
			}
			time.Sleep(HeartbeatInterval)
		} else {
			timeout := make(chan struct{})
			go func() {
				time.Sleep(randomDuration())
				timeout <- struct{}{}
			}()
			select {
			case <-rf.heartbeat:
				// TODO: 检查是否收到AppendEntries请求
				Debugf("ticker %v: case <- rf.aerCh\n", rf.me)
				time.Sleep(randomDuration())
			case <-timeout:
				// 开始竞选
				rf.HoldElection()
			}
			Debugf("ticker %v: rf.state: %v\n", rf.me, rf.state)
		}
	}
}

func (rf *Raft) HoldElection()  {
	// NOTE: 清空aerCh
	// select {
	// case <- rf.aerCh:
	// default:
	// }
	rf.mu.Lock()
	Debugf("before hold: %v\n", rf)
	Debugf("HoldElection-Start: %v\n", rf)
	win := make(chan struct{})
	rf.currentTerm += 1
	// rf.state = Candidate
	// 自己投自己1票,随后会向自身发送RequestVote RPC请求,但因为votedFor已经>=0,不会再加投自己
	rf.votes = 1
	rf.votedFor = rf.me
	args := &RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		// LastLogIndex: rf.log[len(rf.log)-1].index,
		// LastLogTerm: rf.log[len(rf.log)-1].term,
	}
	for i := range rf.peers {
		reply := &RequestVoteReply{}
		go func(i int, reply *RequestVoteReply) {
			rf.sendRequestVote(i, args, reply)
			// rf.mu.Lock()
			// defer rf.mu.Unlock()
			Debugf("HoldElection: me:%v currentTerm:%v reply: %v\n", rf.me, rf.currentTerm, reply)
			if reply.Term > rf.currentTerm {
				rf.coverTerm(reply.Term)
			} else {
				if reply.VoteGranted {
					rf.votes += 1
				}
				if rf.votes > len(rf.peers) / 2{
					win <- struct{}{}
				}
			}
			// ERRO: 未Unlock之前,外部的HoldElection函数返回了,所以这个lock永远锁住了
			// rf.mu.Unlock()
			Debugf("Hold-Unlock: %v\n", rf)
		}(i, reply)
	}
	timeout := make(chan struct{})
	go func() {
		time.Sleep(randomDuration())
		timeout <- struct{}{}
	}()

	rf.mu.Unlock()
	select {
	case <-rf.heartbeat:
		// ERRO: 这部分永远不会运行,why?
		Debugf("HoldElection-Heartbeat: %v\n", rf)
		rf.mu.Lock()
		rf.state = Follower
		rf.mu.Unlock()
	case <- win:
		rf.mu.Lock()
		Debugf("HoldElection-Win: %v\n", rf)
		// 接任成为新leader
		rf.state = Leader
		rf.mu.Unlock()
	case <-timeout:
		Debugf("HoldElection-Fail: %v\n", rf)
		rf.mu.Lock()
		// 竞选失败,恢复Follower
		rf.state = Follower
		rf.mu.Unlock()
	// TODO: case heartbeat
	}
	Debugf("HoldElection-End: %v\n", rf)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	Debugf("Make: rf.state: %v\n", rf.state)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}

func (rf *Raft) String() string {
	return fmt.Sprintf("me: %v, currentTerm: %v, votedFor: %v, state: %v, votes: %v",
		rf.me, rf.currentTerm, rf.votedFor, rf.state, rf.votes)
}

const DebugOpen = false
func Debugf(format string, v ...interface{}) {
	if !DebugOpen {
		return
	}
	log.Printf(format, v...)
}

func (rf *Raft) coverTerm(term int) {
	// NOTE: 只有在rf.mu持有时,才能调用此method
	if rf.currentTerm >= term {
		log.Fatalln("rf.currentTerm >= term")
	}
	Debugf("coverTerm: %v, newTerm: %v", rf, term)
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
	rf.votes = 0
}