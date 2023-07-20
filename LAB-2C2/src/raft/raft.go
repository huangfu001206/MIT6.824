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
	"6.5840/labgob"
	"bytes"
	"fmt"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

var LOG bool = true

// DebugPrintf 日志打印函数
func DebugPrintf(msg string, args ...interface{}) {
	if LOG {
		fmt.Printf(msg, args...)
	}
}

// Log 日志结构体
type Log struct {
	Command interface{}
	Term    int
}

// AppendEntriesArgs 添加日志请求/心跳 结构体
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log // if entries is empty --- heartbeat;else log
	LeaderCommit int
}

// AppendEntriesReply 添加日志/心跳 回复 结构体
type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

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

type licenseType int

// 定义不同的License
const (
	Follower licenseType = iota + 1
	Candidate
	Leader
)

type timerType int

const (
	AppendEntriesTimer timerType = iota
	RequestVoteTimer
)

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []Log
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	timer         *time.Timer
	heartBeatTime time.Duration
	voteBasicTime int32
	license       licenseType
	applyMsgChan  chan ApplyMsg

	ApplyMsgCond *sync.Cond
}

type PersistRaft struct {
	CurrentTerm int
	VotedFor    int
	Log         []Log
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.license == Leader
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.license == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	persistRaft := PersistRaft{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,
	}
	err := e.Encode(persistRaft)
	if err != nil {
		panic("Encode Failed")
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	temp := PersistRaft{}
	if d.Decode(&temp) == nil {
		rf.currentTerm = temp.CurrentTerm
		rf.votedFor = temp.VotedFor
		rf.log = temp.Log
	} else {
		panic("Decode Failed")
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
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

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DebugPrintf("(%v) ********** RequestVote ************\n", rf.me)
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DebugPrintf("(%v) : RequestVote Term : %v  args: %v\n", rf.me, rf.currentTerm, *args)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm {
		rf.newTerm(args.Term)
		reply.Term = rf.currentTerm
	}
	newer := rf.upToDate(args.LastLogIndex, args.LastLogTerm)
	reply.VoteGranted = false
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && newer {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.ResetTimer(RequestVoteTimer)
	}
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
func (rf *Raft) sendRequestVote(server int, agree *int, args *RequestVoteArgs, reply *RequestVoteReply) {
	DebugPrintf("(%v) : sendRequestVote Term : %v\n", rf.me, rf.currentTerm)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	now := time.Now()
	for !ok {
		if time.Now().Sub(now) > 5*time.Millisecond {
			break
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.VoteGranted && reply.Term == rf.currentTerm {
		*agree++
		DebugPrintf("(%v) agree (%v)  agree : %v\n", server, rf.me, *agree)
		if *agree*2 > len(rf.peers) && rf.license == Candidate {
			DebugPrintf("(%v) : be leader, agree : %v, sum : %v\n", rf.me, *agree, len(rf.peers))
			rf.becomeLeaderInit()
		}
	} else if reply.Term > rf.currentTerm {
		rf.newTerm(reply.Term)
	}
}

func (rf *Raft) newTerm(term int) {
	DebugPrintf("(%v) ********** newTerm ************\n", rf.me)
	rf.license = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

// 自己日志更新，返回false
// 否则返回true
func (rf *Raft) upToDate(index int, term int) bool {
	ownLastLogIndex := len(rf.log) - 1
	ownLastLogTerm := rf.log[ownLastLogIndex].Term
	DebugPrintf("upToDate : ownLastLogIndex : %v  ownLastLogTerm : %v, args.LastLogIndex : %v , args.LastLogTerm : %v \n", ownLastLogIndex, ownLastLogTerm, index, term)
	if ownLastLogTerm == term {
		if ownLastLogIndex > index {
			return false
		}
	} else if ownLastLogTerm > term {
		return false
	}
	return true
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.license != Leader {
		return -1, -1, false
	}
	isLeader = true
	index = len(rf.log)
	term = rf.currentTerm
	DebugPrintf("(%v) : Start  newLog : %v\n", rf.me, command)
	rf.log = append(rf.log, Log{
		Term:    rf.currentTerm,
		Command: command,
	})
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
	//rf.mu.Lock()
	//rf.license = Follower
	//rf.persist()
	//rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		select {
		case <-rf.timer.C:
			rf.mu.Lock()
			license := rf.license
			rf.mu.Unlock()
			switch license {
			case Follower:
				fallthrough
			case Candidate:
				rf.VoteProcess() // 超时，进行投票选举新的leader
				DebugPrintf("(%v) : be Candidate, Term : %v\n", rf.me, rf.currentTerm)
			case Leader:
				rf.AppendEntriesProcess() // leader发送心跳或者日志拷贝请求
				DebugPrintf("(%v) : be Leader, Term : %v\n", rf.me, rf.currentTerm)
			}
		}
	}
}

func (rf *Raft) VoteProcess() {
	if rf.killed() == false {
		DebugPrintf("(%v) ********** VoteProcess ************\n", rf.me)
		rf.mu.Lock()
		rf.becomeCandidateInit()
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.log) - 1,
			LastLogTerm:  rf.log[len(rf.log)-1].Term,
		}
		var agree = 1
		rf.mu.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				reply := &RequestVoteReply{}
				go rf.sendRequestVote(i, &agree, args, reply)
			}
		}
	}
}

func (rf *Raft) findFirstIndexByTerm(args *AppendEntriesArgs) int {
	var i = 0
	term := rf.log[args.PrevLogIndex].Term
	DebugPrintf("******** PreLogIndex : %v  , len : %v,  rf.log : %v *********\n", args.PrevLogIndex, len(rf.log), rf.log)
	for i = args.PrevLogIndex - 1; rf.log[i].Term == term; i-- {
	}
	DebugPrintf("******* originTerm : %v  findTerm : %v  res : %v********\n", term, rf.log[i+1].Term, i+1)
	return i + 1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DebugPrintf("(%v) ********** AppendEntries ************\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		//参数任期小于当前server任期，返回false
		DebugPrintf("(%v)<---(%v) : args.Term(%v) < rf.currentTerm(%v) \n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	} else {
		//参数任期大于当前任期，同步任期，并重置计时器
		rf.newTerm(args.Term)
		reply.Term = rf.currentTerm
		rf.ResetTimer(RequestVoteTimer)
	}
	if args.PrevLogIndex >= len(rf.log) {
		reply.XLen = len(rf.log)
		DebugPrintf("(%v) <--- (%v) : reply.XLen : %v\n", rf.me, args.LeaderId, reply.XLen)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		reply.XIndex = rf.findFirstIndexByTerm(args)
		DebugPrintf("(%v) <--- (%v) : reply.XTerm : %v  reply.XIndex : %v\n", rf.me, args.LeaderId, reply.XTerm, reply.XIndex)
		return
	}

	DebugPrintf("(%v) : AppendEntries Success Term : %v  args : %v\n", rf.me, rf.currentTerm, *args)
	//匹配, Figure 2 AppendEntries RPC 3-4
	reply.Success = true
	for index, value := range args.Entries {
		i := args.PrevLogIndex + 1 + index
		if i >= len(rf.log) {
			rf.log = append(rf.log, value)
		} else if rf.log[i].Term != value.Term {
			rf.log[i] = value
			rf.log = rf.log[:i+1]
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		rf.ApplyMsgCond.Signal()
	}
}
func (rf *Raft) solveAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DebugPrintf("(%v) ********** solveAppendEntriesReply ************\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.license == Leader && rf.dead != 1 {
		DebugPrintf("(%v) : reply.Term : %v  rf.currentTerm : %v \n", rf.me, reply.Term, rf.currentTerm)
		if reply.Term > rf.currentTerm {
			rf.newTerm(reply.Term)
			return true
		}
		if reply.Term == rf.currentTerm {
			if reply.Success == false {
				if reply.XLen != 0 {
					rf.nextIndex[server] = reply.XLen
					DebugPrintf("(%v) <--- (%v) : reply.XLen != 0  nextIndex : %v \n", rf.me, server, reply.XLen)
					return false
				}
				index := rf.binarySearchTerm(reply.XTerm)
				if index == -1 {
					// 未找到相同周期的日志
					rf.nextIndex[server] = reply.XIndex
					DebugPrintf("(%v) <--- (%v) : nextIndex : %v \n", rf.me, server, reply.XIndex)
				} else {
					rf.nextIndex[server] = index
					DebugPrintf("(%v) <--- (%v) : rf.binarySearchTerm(reply.XTerm)   nextIndex : %v \n", rf.me, server, index)
				}
				return false
			}
			DebugPrintf("(%v) : server %v logMatchSuccess\n", rf.me, server)
			//日志匹配成功，更新matchIndex以及 nextIndex
			logLen := args.PrevLogIndex + len(args.Entries)
			rf.matchIndex[server] = max(rf.matchIndex[server], logLen)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			rf.commitIndexCheck()
			return true
		}
	}
	return true
}

func (rf *Raft) prepareAppendEntriesArgs(server int) (bool, *AppendEntriesArgs) {
	DebugPrintf("(%v) ********** prepareAppendEntriesArgs ************\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.license == Leader && rf.dead != 1 {
		if rf.nextIndex[server] <= 0 {
			return false, &AppendEntriesArgs{}
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
			Entries:      rf.log[rf.nextIndex[server]:],
			LeaderCommit: rf.commitIndex,
		}
		return true, args
	}
	return false, &AppendEntriesArgs{}
}

func (rf *Raft) sendAppendEntries(server int) {
	DebugPrintf("(%v) ********** sendAppendEntries ************\n", rf.me)
	for rf.killed() == false && rf.isLeader() {
		//准备所需发送的参数
		flag, args := rf.prepareAppendEntriesArgs(server)
		if flag == false {
			return
		}
		reply := &AppendEntriesReply{}

		//请求发送
		DebugPrintf("(%v) : sendAppendEntries Term : %v  args : %v\n", rf.me, rf.currentTerm, *args)
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		for !ok && rf.isLeader() {
			time.Sleep(5 * time.Millisecond)
			ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		}

		//处理回复
		if rf.solveAppendEntriesReply(server, args, reply) {
			break
		}
	}
}
func (rf *Raft) AppendEntriesProcess() {
	if rf.killed() == false {
		DebugPrintf("(%v) ********** AppendEntriesProcess ************\n", rf.me)
		rf.mu.Lock()
		rf.ResetTimer(AppendEntriesTimer)
		rf.persist()
		rf.mu.Unlock()
		//向每个服务器发送AppendEntries RPC
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go rf.sendAppendEntries(i)
			}
		}
	}
}

func (rf *Raft) becomeCandidateInit() {
	DebugPrintf("(%v) ********** becomeCandidateInit ************\n", rf.me)
	rf.license = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.ResetTimer(RequestVoteTimer)
}

func (rf *Raft) becomeLeaderInit() {
	DebugPrintf("(%v) ********** becomeLeaderInit ************\n", rf.me)
	rf.license = Leader
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.log)
	}
	rf.timer.Reset(0)
	rf.persist()
}

func (rf *Raft) ResetTimer(types timerType) {
	defer rf.persist()
	switch types {
	case RequestVoteTimer:
		DebugPrintf("(%v) : resetTimer---RequestVoteTimer, Term : %v\n", rf.me, rf.currentTerm)
		rf.timer.Reset(time.Duration(rf.voteBasicTime+rand.Int31()%200) * time.Millisecond)
	default:
		DebugPrintf("(%v) : resetTimer---HeartBeatTime, Term : %v\n", rf.me, rf.currentTerm)
		rf.timer.Reset(time.Duration(rf.heartBeatTime))
	}
}

func (rf *Raft) sendCommitMsg2ApplyCh(msg ApplyMsg) {
	DebugPrintf("(%v) sendCommitMsg2ApplyCh  content: %v\n", rf.me, msg)
	rf.applyMsgChan <- msg
}

func (rf *Raft) commitIndexCheck() {
	DebugPrintf("(%v) : *********** commitIndexCheck ************\n", rf.me)
	index := rf.findNewCommitIndex()
	DebugPrintf("(%v) : commitIndexCheck  Term : %v  CommitIndex : %v  newCommitIndex : %v\n", rf.me, rf.currentTerm, rf.commitIndex, index)
	if index != -1 {
		rf.commitIndex = index
		rf.timer.Reset(0)
		rf.persist()
		rf.ApplyMsgCond.Signal()
	}
}

func (rf *Raft) findNewCommitIndex() int {
	logLength := len(rf.log)
	serverNum := len(rf.peers)
	DebugPrintf("(%v) : findNewCommitIndex  matchIndex : %v \n", rf.me, rf.matchIndex)
	for i := logLength - 1; i > rf.commitIndex; i-- {
		sum := 0
		for index, value := range rf.matchIndex {
			if value >= i || index == rf.me {
				sum++
			}
		}
		if sum*2 > serverNum && rf.log[i].Term == rf.currentTerm {
			return i
		}
	}
	return -1
}

func (rf *Raft) commit2ApplyCh() {
	rf.ApplyMsgCond.L.Lock()
	defer rf.ApplyMsgCond.L.Unlock()
	for rf.killed() == false {
		rf.ApplyMsgCond.Wait()
		DebugPrintf("(%v) : commit2ApplyCh Term : %v  lastApplied : %v  commitIndex : %v\n", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
		//进行日志提交
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			rf.sendCommitMsg2ApplyCh(ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			})
		}
		rf.lastApplied = max(rf.lastApplied, rf.commitIndex)
		rf.persist()
	}
}
func (rf *Raft) binarySearchTerm(term int) int {
	left := 1
	right := len(rf.log) - 1
	for left < right {
		mid := (left + right + 1) / 2
		tempTerm := rf.log[mid].Term
		if tempTerm > term {
			right = mid - 1
		} else {
			left = mid
		}
	}
	if rf.log[left].Term == term {
		return left
	}
	return -1
}

func min(x int, y int) int {
	if x > y {
		return y
	}
	return x
}

func max(x int, y int) int {
	if x > y {
		return x
	}
	return y
}

// Make the service or tester wants to create a Raft server. the ports
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
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.license = Follower

	rf.log = append(rf.log, Log{
		Term:    -1,
		Command: "Start",
	})

	rf.heartBeatTime = time.Duration(100) * time.Millisecond
	rf.voteBasicTime = 250
	rf.timer = time.NewTimer(time.Duration(rf.voteBasicTime+rand.Int31()%200) * time.Millisecond)
	rf.applyMsgChan = applyCh

	rf.mu.Unlock()

	rf.ApplyMsgCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	//检查是否需要进行日志提交
	//如果满足条件，则进行提交
	go rf.commit2ApplyCh()

	return rf
}
