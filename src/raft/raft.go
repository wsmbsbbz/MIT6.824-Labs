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
	"bytes"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"math/rand"

	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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
	Leader    = 1
	Candidate = 2
	Follower  = 3
	// timing
	MinTick           = 200 * time.Millisecond
	TickInterval      = 300 * time.Millisecond
	HeartbeatInterval = 100 * time.Millisecond
	timerLoop         = 10 * time.Millisecond
)

type logEntry struct {
	// NOTE: protocol: 一个正确的Term号从0开始单增,在Make中初始化,但至少在Term号为1时才能
	// 选出第一个leader,所以logEntry的Term应该从1开始,并保持单增
	Index   int
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
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
	votedFor    int // 在当前term未投票时,votedFor = -1
	// NOTE: protocol: log初始时有一个nil条目,目的是保持log的下标和paper中的index一致
	log []logEntry
	// volatile state on all servers:
	commitIndex  int
	secureCommit int
	lastApplied  int
	// volatile state on leaders:
	matchIndex []int

	// for leader election:
	state     int // leader, candidate, follower
	votes     int
	heartbeat time.Time
	election  time.Time // NOTE: 每次收到AppendEntries RPC,会重置election

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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// NOTE: protocol: 必须已经hold rf.mu,再调用此方法
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []logEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		Debugf("readPersist: nil\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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
	// WARR: 此处hold rf.mu则会导致程序无法继续推进
	rf.TrimToIndex(index)
}

// WARR: 原本需要先hold rf.mu,再调用此方法,但是在Snapshot方法中若hold rf.mu则程序无法推进
func (rf *Raft) TrimToIndex(index int) {
	Debugf("TrimToIndex: before trim: index: %v log: %v\n", index, rf.log)
	idx := rf.searchLogIndex(index)
	l := []logEntry{rf.log[0]}
	l = append(l, rf.log[idx+1:]...)
	rf.log = l
	Debugf("TrimToIndex: return log: %v\n", rf.log)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	// NOTE: protocol: 所有的CandidateId应该大于或等于0,才能保证不和votedFor的默认值-1冲突
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// NOTE: protocol: 必须已经hold rf.mu,再调用此方法
func (rf *Raft) searchLogIndex(target int) int {
	Debugf("searchLogIndex: target: %v\n", target)
	l, r := 0, len(rf.log)
	for l < r {
		m := l + (r-l)/2
		if rf.log[m].Index < target {
			l = m + 1
		} else {
			r = m
		}
	}
	return min(l, rf.log[len(rf.log)-1].Index)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	Debugf("RequestVote-Start: Raft: %v#, args: %v\n", rf, args)
	defer func() {
		reply.Term = rf.currentTerm
		Debugf("RequestVote-End: Raft: %v#, reply: %v\n", rf, reply)
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
	// NOTE: check whether the candidate's log is up-to-date
	if rf.votedFor >= 0 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}
	lastLog := rf.log[len(rf.log)-1]
	lastLogTerm, lastLogIndex := lastLog.Term, lastLog.Index
	if (lastLogTerm > args.LastLogTerm) ||
		(lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		reply.VoteGranted = false
		return
	}
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	Debugf("AppendEntries-Start: Raft: %v#, args: %v\n", rf, args)
	defer func() {
		reply.Term = rf.currentTerm
		Debugf("AppendEntries-End: Raft: %v#, reply: %v\n", rf, reply)
		rf.mu.Unlock()
	}()

	// Rules for All Servers:
	// NOTE: rule 1 has been implemented in committer
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
	lastLog := rf.log[len(rf.log)-1]
	prevLogIndex := rf.searchLogIndex(args.PrevLogIndex)
	if args.PrevLogIndex > lastLog.Index || rf.log[prevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	// 3. If an existing entry conflicts with a new one (same index but different
	// terms), delete the existing entry and all that follow it ($5.3)
	i, j := 0, 0
	for i < len(args.Entries) && j < len(rf.log) {
		if args.Entries[i].Index < rf.log[j].Index {
			i++
		} else if args.Entries[i].Index > rf.log[j].Index {
			j++
		} else {
			if !checkLogEqual(args.Entries[i], rf.log[j]) {
				rf.log = rf.log[:j]
				break
			}
			i++
			j++
		}
	}
	// 4. Append any new entries not already in the log
	Debugf("AppendEntries: rf.log: %v#, args.Entries[%v:]: %v\n", rf.log, i, args.Entries[i:])
	rf.log = append(rf.log, args.Entries[i:]...)
	// 5. If leaderCommit > commitIndex, set commitIndex = min(
	// leaderCommit, index of last new entry)
	rf.persist()
	rf.secureCommit = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
	reply.Success = true
	rf.tickerReset()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	// NOTE: protocol: 虽然successCnt不是Raft结构体的字段,但是也要先hold rf.mu.lock再更改
	rf.mu.Lock()
	index, term, isLeader := rf.log[len(rf.log)-1].Index+1, rf.currentTerm, rf.state == Leader
	if rf.state != Leader {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	Debugf("Start-Start: me: %v command: %v\n", rf.me, command)
	log := logEntry{rf.log[len(rf.log)-1].Index + 1, rf.currentTerm, command}
	rf.log = append(rf.log, log)
	rf.persist()

	Debugf("Start: return\n")
	rf.mu.Unlock()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
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
				Debugf("me: %v\n", rf)
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

// NOTE: protocol: 此方法应该在一个goroutine中独立运行
func (rf *Raft) committer() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state == Leader {
			n := len(rf.peers)
			commitNums := make([]int, n)
			copy(commitNums, rf.matchIndex)
			sort.Ints(commitNums)
			rf.commitIndex = rf.secureCommit
			idx := rf.searchLogIndex(commitNums[n/2])
			if rf.log[idx].Term == rf.currentTerm {
				rf.secureCommit = commitNums[n/2]
			}
		}
		for rf.commitIndex < rf.secureCommit {
			// TODO: 先apply,再发送到applyCh
			rf.commitIndex++
			idx := rf.searchLogIndex(rf.commitIndex)
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[idx].Command,
				CommandIndex: rf.log[idx].Index,
			}
			Debugf("commiter: %v\n", applyMsg)
			rf.applyCh <- applyMsg
			Debugf("commiter: %v accepted\n", applyMsg)
		}
		rf.mu.Unlock()
		time.Sleep(timerLoop)
	}
}

// NOTE: protocol: 必须已经hold rf.mu,再调用此方法
func (rf *Raft) sendHeartbeats() {
	for i := range rf.peers {
		idx := rf.searchLogIndex(rf.matchIndex[i])
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.matchIndex[i],
			PrevLogTerm:  rf.log[idx].Term,
			Entries:      rf.log[idx+1:],
			LeaderCommit: rf.secureCommit,
		}
		reply := &AppendEntriesReply{}
		go func(i int) {
			rf.sendAppendEntries(i, args, reply)
			rf.mu.Lock()
			// in case of term confusion
			if rf.currentTerm != args.Term {
				rf.mu.Unlock()
				return
			}
			if reply.Term > rf.currentTerm {
				rf.coverTerm(reply.Term)
			}
			if reply.Success {
				Debugf("before-max: %v's matchIndex: %v", rf.me, rf.matchIndex)
				if len(args.Entries) > 0 {
					rf.matchIndex[i] = max(rf.matchIndex[i], args.Entries[len(args.Entries)-1].Index)
				}
				Debugf("after-max: %v's matchIndex: %v", rf.me, rf.matchIndex)
			}
			rf.mu.Unlock()
		}(i)
	}
}

// NOTE: protocol: 必须已经hold rf.mu,再调用此方法
func (rf *Raft) holdElection() {
	Debugf("holdElection-Start: %v\n", rf)
	rf.tickerReset()
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votes = 0
	rf.votedFor = -1
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].Index,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	for i := range rf.peers {
		reply := &RequestVoteReply{}
		go func(i int, reply *RequestVoteReply) {
			rf.sendRequestVote(i, args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			Debugf("RequestVoteReply: me:%v reply: %v\n", rf.me, reply)
			// in case of term confusion
			if rf.currentTerm != args.Term {
				return
			}
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
			if rf.votes > len(rf.peers)/2 {
				rf.state = Leader
				Debugf("Win the election: %v\n", rf)
				for i := range rf.peers {
					rf.matchIndex[i] = 0
				}
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

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
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
	// // NOTE: 添加了个无用的logEntry,以便于下标和paper对齐
	rf.log = append(rf.log, logEntry{0, 0, nil})
	rf.applyCh = applyCh
	// NOTE: matchIndex中元素的初始值应为0
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.beater()
	go rf.committer()

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

func checkLogEqual(a, b logEntry) bool {
	if a.Term != b.Term {
		return false
	}
	return a.Index == b.Index
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// NOTE: protocol: 必须已经hold rf.mu,再调用此方法
func (rf *Raft) String() string {
	return fmt.Sprintf("me: %v, currentTerm: %v, votedFor: %v, state: %v, votes: %v, commitIndex: %v, log: %v, matchIndex :%v",
		rf.me, rf.currentTerm, rf.votedFor, rf.state, rf.votes, rf.secureCommit, rf.log, rf.matchIndex)
}

func (applyMsg ApplyMsg) String() string {
	return fmt.Sprintf("CommandValid: %v, Command: %v, CommandIndex: %v",
		applyMsg.CommandValid, applyMsg.Command, applyMsg.CommandIndex)
}

func (aeArgs AppendEntriesArgs) String() string {
	return fmt.Sprintf("Term: %v, LeaderId: %v, PrevLogIndex: %v, PrevLogTerm: %v, Entries: %v, LeaderCommit: %v",
		aeArgs.Term, aeArgs.LeaderId, aeArgs.PrevLogIndex, aeArgs.PrevLogTerm, aeArgs.Entries, aeArgs.LeaderCommit)
}

func (aeReply AppendEntriesReply) String() string {
	return fmt.Sprintf("Term: %v, Success: %v", aeReply.Term, aeReply.Success)
}

func (rvArgs RequestVoteArgs) String() string {
	return fmt.Sprintf("Term: %v, CandidateId: %v, LastLogIndex: %v, LastLogTerm: %v",
		rvArgs.Term, rvArgs.CandidateId, rvArgs.LastLogIndex, rvArgs.LastLogTerm)
}

func (rvReply RequestVoteReply) String() string {
	return fmt.Sprintf("Term: %v, VoteGranted: %v", rvReply.Term, rvReply.VoteGranted)
}

// func (entry logEntry) String() string {
// 	return fmt.Sprintf("Term: %v, Command: %v", entry.Term, entry.Command)
// }

const DebugOpen = false

func Debugf(format string, v ...interface{}) {
	log.SetFlags(log.Lmicroseconds)
	if !DebugOpen {
		return
	}
	log.Printf(format, v...)
}
