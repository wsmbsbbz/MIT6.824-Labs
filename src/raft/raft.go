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
	Candidate = 2
	Follower = 3
	// timing
	MinTick = 200 * time.Millisecond
	TickInterval = 300 * time.Millisecond
	HeartbeatInterval = 100 * time.Millisecond
	timerLoop = 10 * time.Millisecond
)

type logEntry struct {
	// 不需要Index字段,因为term增加并不会重置Index,所以只需要通过logEntry在log[]数组中的下
	// 标来判断
	// NOTE: protocol: 一个正确的Term号从0开始单增,在Make中初始化,但至少在Term号为1时才能
	// 选出第一个leader,所以logEntry的Term应该从1开始,并保持单增
	Term int
	Command interface{}
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
	votedFor int // 在当前term未投票时,votedFor = -1
	// NOTE: protocol: log初始时有一个nil条目,目的是保持log的下标和paper中的index一致
	log []logEntry // NOTE: temporary
	// volatile state on all servers:
	commitIndex int
	lastApplied int
	// volatile state on leaders:
	nextIndex []int
	matchIndex []int

	// for leader election:
	state int // leader, candidate, follower
	votes int
	heartbeat time.Time
	election time.Time // NOTE: 每次收到AppendEntries RPC,会重置election

	// for communicating to clients/testers
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// return term, isleader
	// Your code here (2A).
	rf.mu.Lock()
	term, isLeader := rf.currentTerm, rf.state == Leader
	Debugf("GetState: %v\n", rf)
	rf.mu.Unlock()
	return term, isLeader
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
	// NOTE: protocol: 所有的CandidateId应该大于或等于0,才能保证不和votedFor的默认值-1冲突
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
	Debugf("RequestVote-Start: %v\n", rf)
	defer func() {
		reply.Term = rf.currentTerm
		Debugf("RequestVote-End: %v\n", rf)
		rf.mu.Unlock()
	}()

	// Rules for All Servers:
	// 1. TODO: if commitIndex > lastApplied, increment lastApplied and apply it
	// 2. if args.Term > rf.currentTerm, convert to follower
	if args.Term > rf.currentTerm {
		// NOTE: 接受任何RPC请求时,发现currentTerm < args.Term,需要更新currentTerm并转换为follower
		rf.coverTerm(args.Term)
	}
	// Rules for RequestVote RPC:
	// 1. reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	// 2. if votedFor is null or candidateId, and candidate's log is at least 
	// up-to-date as receiver's log, grant vote
	if rf.votedFor >= 0 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}
	lastLogTerm := rf.log[len(rf.log)-1].Term
	if lastLogTerm > args.Term {
		reply.VoteGranted = false
		return
	}
	lastLogIndex := len(rf.log) - 1
	if lastLogIndex > len(rf.log) - 1 {
		reply.VoteGranted = false
		return
	}
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
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

// WARR: 即时是leader,也应该服从上面的那个规则,所以leader代码写到下面
// if rf.state == Leader {
// 	Debugf("AppendEntries: rf.state is Leader, just reply\n")
// 	reply.Success = args.LeaderId == rf.me
// 	return
// }
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	Debugf("AppendEntries-Start: %v", rf)
	Debugf("AppendEntries-args: %v", args)
	defer func() {
		reply.Term = rf.currentTerm
		Debugf("AppendEntries-End: %v", rf)
		rf.mu.Unlock()
	}()

	// Rules for All Servers:
	// 1. TODO: if commitIndex > lastApplied, increment lastApplied and apply it
	// 2. if args.Term > rf.currentTerm, convert to follower
	if args.Term > rf.currentTerm {
		rf.coverTerm(args.Term)
	}

	// Receiver implementation:
	// 1. reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} 
	// 2. reply false if log doesn't contain an entry at prevLogIndex whose term
	// matches prevLogTerm
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term !=
		args.PrevLogTerm {
			reply.Success = false
			return
	}
	// 3. If an existing entry conflicts with a new one (same index but different
	// terms), delete the existing entry and all that follow it ($5.3)
	var i int
	for i = 0; i < len(args.Entries) && (args.PrevLogIndex + 1 + i) < len(rf.log); i++ {
		Debugf("i: %v\n", i)
		if rf.log[args.PrevLogIndex+1+i].Term != args.Entries[i].Term {
			rf.log = rf.log[:args.PrevLogIndex+i+1]
			break
			// rf.log[args.PrevLogIndex+1+i] = args.Entries[i]
		}
	}
	// 4. Append any new entries not already in the log
	Debugf("Implementation 4: rf.log: %v, args.Entries[%v:]: %v\n", rf.log, i, args.Entries[i:])
	rf.log = append(rf.log, args.Entries[i:]...)
	// 5. If leaderCommit > commitIndex, set commitIndex = min(
	// leaderCommit, index of last new entry)
	for args.LeaderCommit > rf.commitIndex {
		// WARR: 因为leader保证发过来的log绝对能包含LeaderCommit的index的log,所以不需要
		// 使用min()函数
		// rf.commitIndex = min(args.LeaderCommit, len(rf.log) - 1)
		rf.commitIndex++
		applyMsg := ApplyMsg {
			CommandValid: true,
			Command: rf.log[rf.commitIndex].Command,
			CommandIndex: rf.commitIndex,
		}
		Debugf("Follower %v's applyMsg: %v\n and it's log: %v", rf.me, applyMsg, rf.log)
		rf.applyCh <- applyMsg
	}
	reply.Success = true
	rf.tickerReset()
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
	index, term, isLeader := -1, -1, false

	// Your code here (2B).
	// NOTE: protocol: 虽然successCnt不是Raft结构体的字段,但是也要先hold rf.mu.lock再更改
	Debugf("Start-Start: command: %v\n", command)
	rf.mu.Lock()
	if rf.state != Leader {
		index, term, isLeader = len(rf.log), rf.currentTerm, false

		rf.mu.Unlock()
		return index, term, isLeader
	}

	successCnt := 0
	cond := sync.NewCond(&rf.mu)
	rf.log = append(rf.log, logEntry{rf.currentTerm, command})
	rf.nextIndex[rf.me] = len(rf.log)
	rf.matchIndex[rf.me] = len(rf.log) - 1

	for i := range rf.peers {
		go func(i int) {
			for {
				rf.mu.Lock()
				args := &AppendEntriesArgs {
					Term: rf.currentTerm,
					LeaderId: rf.me,
					PrevLogIndex: rf.matchIndex[i],
					PrevLogTerm: rf.log[rf.matchIndex[i]].Term,
					Entries: rf.log[rf.matchIndex[i]+1:],
					LeaderCommit: rf.commitIndex,
				}
				Debugf("Start: i: %v, args: %v\n", i, args)
				// ERRO: 若不lock,会导致leader只能单线程等待RPC回复,性能极差
				rf.mu.Unlock()
				// ERRO: 在这个未lock的空隙,rf.nextIndex的值就有可能发生改变,会导致args
				// 的内容过时
				reply := &AppendEntriesReply{}
				rf.sendAppendEntries(i, args, reply)
				Debugf("Start: i: %v, reply: %v\n", i, reply)
				// TODO: 处理log
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.coverTerm(reply.Term)
				}
				if reply.Success {
					successCnt++
					Debugf("Start: successCnt: %v\n", successCnt)
					cond.Broadcast()
					// rf.nextIndex[i] += len(args.Entries)
					// rf.matchIndex[i] = rf.nextIndex[i] - 1
					rf.matchIndex[i] += len(args.Entries)
					Debugf("leader.nextIndex: %v\n", rf.nextIndex)
					Debugf("leader.matchIndex: %v\n", rf.matchIndex)
					rf.mu.Unlock()
					break
				} else {
					rf.nextIndex[i] = max(0, rf.nextIndex[i] - 1)
					rf.matchIndex[i] = max(0, rf.matchIndex[i] - 1)
					time.Sleep(timerLoop)
					// TODO: 直接retry,不要等到下一次
				}
				// NOTE: 收到reply时,当前server可能已经退位了,需要处理这种情况
				rf.mu.Unlock()
			}
		}(i)
	}

	for successCnt <= len(rf.peers) / 2 {
		Debugf("Start: waiting for exit cond-loop\n")
		cond.Wait()
	}
	index, term, isLeader = len(rf.log) - 1, rf.currentTerm, true
	applyMsg := ApplyMsg {
		CommandValid: true,
		Command: command,
		CommandIndex: index,
	}
	rf.commitIndex = index
	Debugf("Start: before send applyMsg: %v\n", applyMsg)
	rf.applyCh <- applyMsg
	Debugf("Start: applyMsg accepted\n")
	rf.mu.Unlock()

	Debugf("Start: return\n")
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
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.state == Follower || rf.state == Candidate {
			if time.Now().After(rf.election) {
				Debugf("ticker: election expires\n")
				rf.holdElection()
			}
		}
		rf.mu.Unlock()
		time.Sleep(timerLoop)
	}
}

// NOTE: protocol: 此方法应该在一个goroutine中独立运行
func (rf *Raft) beater() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state == Leader {
			// NOTE: 每当rf.heartbeat到期后,就发起一轮heartbeat
			if time.Now().After(rf.heartbeat) {
				Debugf("beater: heartbeat expires\n")
				Debugf("leader: %v\n", rf)
				rf.beaterReset()
				rf.sendHeartbeats()
			}
		}
		rf.mu.Unlock()
		time.Sleep(timerLoop)
	}
}

// NOTE: protocol: 必须已经hold rf.mu,再调用此方法
func (rf *Raft) sendHeartbeats() {
	for i := range rf.peers {
		args := &AppendEntriesArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
			PrevLogIndex: rf.matchIndex[i],
			PrevLogTerm: rf.log[rf.matchIndex[i]].Term,
			// NOTE: 发送空log,不需要修改AppendEntries代码
			Entries: []logEntry{},
			LeaderCommit: rf.commitIndex,
		}
		reply := &AppendEntriesReply{}
		go func(i int) {
			rf.sendAppendEntries(i, args, reply)
			// TODO: 处理log
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.coverTerm(reply.Term)
			}
			// NOTE: 收到reply时,当前server可能已经退位了,需要处理这种情况
			rf.mu.Unlock()
		}(i)
	}
}

// NOTE: protocol: 必须已经hold rf.mu,再调用此方法
func (rf *Raft) holdElection()  {
	Debugf("holdElection-Start: %v\n", rf)
	rf.tickerReset()
	rf.state = Candidate
	rf.currentTerm += 1
	// 自己投自己1票,随后会向自身发送RequestVote RPC请求,但因为votedFor已经>=0,不会再加投自己
	rf.votes = 1
	rf.votedFor = rf.me
	args := &RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm: rf.log[len(rf.log)-1].Term,
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		reply := &RequestVoteReply{}
		go func(i int, reply *RequestVoteReply) {
			rf.sendRequestVote(i, args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			Debugf("RequestVoteReply: me:%v reply: %v\n", rf.me, reply)
			if reply.Term > rf.currentTerm {
				rf.coverTerm(reply.Term)
				return
			}
			if rf.state == Leader {
				return
			}
			if reply.VoteGranted {
				rf.votes += 1
			}
			if rf.votes > len(rf.peers) / 2 {
				rf.state = Leader
				// TODO: 赢得选举后,先更新nextIndex和matchIndex信息
				for i := range rf.peers {
					rf.nextIndex[i] = len(rf.log)
				}
				Debugf("Win the election: %v\n", rf)
				// TODO: 赢得选举转化成leader后,应该立刻发出heartbeat
				rf.sendHeartbeats()
			}
		}(i, reply)
	}
	Debugf("holdElection-End: %v\n", rf)
}

// NOTE: protocol: 必须已经hold rf.mu,再调用此方法
func (rf *Raft) beaterReset() {
	rf.heartbeat = time.Now().Add(HeartbeatInterval)
}

// NOTE: protocol: 必须已经hold rf.mu,再调用此方法
func (rf *Raft) tickerReset() {
	rand.Seed(time.Now().UnixNano())
	d := MinTick + time.Duration(rand.Intn(int(TickInterval)))
	rf.election = time.Now().Add(MinTick + d)
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
	rf.log = append(rf.log, logEntry{0, nil})
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(rf.peers))
	// NOTE: nextIndex中元素的初始值应为1
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	// NOTE: matchIndex中元素的初始值应为0
	rf.matchIndex = make([]int, len(rf.peers))
	// // NOTE: 添加了个无用的logEntry,以便于下标和paper对齐
	// rf.log = append(rf.log, logEntry{0, nil})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.beater()

	return rf
}

// NOTE: protocol: 必须已经hold rf.mu,再调用此方法
func (rf *Raft) coverTerm(term int) {
	if rf.currentTerm >= term {
		log.Fatalln("rf.currentTerm >= term")
	}
	Debugf("coverTerm: %v, newTerm: %v", rf, term)
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
	rf.votes = 0
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// NOTE: protocol: 必须已经hold rf.mu,再调用此方法
func (rf *Raft) String() string {
	return fmt.Sprintf("me: %v, currentTerm: %v, votedFor: %v, state: %v, votes: %v, commitIndex: %v, log: %v, nextIndex: %v, matchIndex :%v",
		rf.me, rf.currentTerm, rf.votedFor, rf.state, rf.votes, rf.commitIndex, rf.log, rf.nextIndex, rf.matchIndex)
}

const DebugOpen = true
func Debugf(format string, v ...interface{}) {
	log.SetFlags(log.Lmicroseconds)
	if !DebugOpen {
		return
	}
	log.Printf(format, v...)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}