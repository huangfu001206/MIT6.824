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
	"fmt"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type Log struct {
	Command interface{}
	Term    int
}
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log // if entries is empty --- heartbeat;else log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
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

const (
	Follower licenseType = iota + 1
	Candidate
	Leader
)

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
	agreeCount    atomic.Int32
	applyMsgChan  *chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.license == Leader
	rf.mu.Unlock()
	// Your code here (2A).
	return term, isleader
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	if rf.currentTerm > args.Term || rf.commitIndex > args.LastLogIndex {
		reply.Term = -1
		return
	} else if rf.currentTerm == args.Term {
		if (rf.log[len(rf.log)-1].Term > args.LastLogTerm) ||
			(rf.log[len(rf.log)-1].Term == args.LastLogTerm && len(rf.log)-1 >= args.LastLogIndex) {
			return
		}
	} else {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.license = Follower
	}
	reply.Term = rf.currentTerm
	if rf.votedFor == -1 {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}
	if rf.license == Follower {
		rf.timer.Reset(rf.heartBeatTime)
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, group *sync.WaitGroup) bool {
	var ok = false
	now := time.Now()
	for !ok {
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
		if time.Now().Sub(now) > time.Duration(2)*time.Millisecond {
			break
		}
	}
	rf.mu.Lock()
	if reply.VoteGranted && rf.currentTerm == reply.Term {
		rf.agreeCount.Add(1)
		if int(rf.agreeCount.Load())*2 >= len(rf.peers) && rf.license == Candidate {
			rf.becomeLeaderInit()
		}
	}
	if reply.Term > rf.currentTerm || reply.Term == -1 {
		rf.license = Follower
		rf.timer.Reset(rf.heartBeatTime)
	}
	rf.mu.Unlock()
	group.Done()
	return ok
}

func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, group *sync.WaitGroup) bool {
	ok := rf.peers[server].Call("Raft.ReceiveMsgFromLeader", args, reply)
	rf.mu.Lock()
	if rf.currentTerm < reply.Term || reply.Term == -1 {
		rf.license = Follower
		rf.timer.Reset(rf.heartBeatTime)
	}
	rf.mu.Unlock()
	group.Done()
	return ok
}

func (rf *Raft) sendLogCopyRequest(server int, agreeNum *int32, sumNum *int32, flag *atomic.Bool, resChan *chan int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.nextIndex[server] <= 0 {
			rf.mu.Unlock()
			break
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
			Entries:      rf.log[rf.nextIndex[server]:],
			LeaderCommit: rf.commitIndex,
		}
		fmt.Printf("(%v) : sendLogCopyRequest: %v\n", rf.me, args)
		rf.mu.Unlock()
		reply := AppendEntriesReply{
			Success: false,
		}
		ok := false
		for !ok {
			ok = rf.peers[server].Call("Raft.ReceiveMsgFromLeader", &args, &reply)
			if ok {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		fmt.Printf("reply : %v\n", reply)
		if reply.Success {
			atomic.AddInt32(agreeNum, 1)
			rf.mu.Lock()
			rf.matchIndex[server] = args.PrevLogIndex
			rf.nextIndex[server] = args.PrevLogIndex + 1
			rf.mu.Unlock()
			break
		}
		rf.mu.Lock()
		rf.nextIndex[server]--
		if rf.nextIndex[server] <= 0 || reply.Term == -1 {
			rf.nextIndex[server] = 0
			rf.mu.Unlock()
			break
		}
		if rf.currentTerm < reply.Term {
			rf.mu.Unlock()
			break
		}

		rf.mu.Unlock()
	}
	atomic.AddInt32(sumNum, 1)
	if atomic.LoadInt32(agreeNum)*2 >= int32(len(rf.peers)) && !flag.Load() {
		*resChan <- 1
		flag.Store(true)
	}
	if atomic.LoadInt32(sumNum) >= int32(len(rf.peers)) && !flag.Load() {
		*resChan <- 0
		flag.Store(true)
	}
}

func (rf *Raft) becomeLeaderInit() {
	rf.license = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.timer.Reset(0)
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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.license != Leader {
		return -1, -1, false
	}
	defer func() {
		go rf.logCopyReqProcess(index)
	}()
	fmt.Printf("(%v) Start\n", rf.me)
	term = rf.currentTerm
	index = len(rf.log)
	log := Log{
		Term:    term,
		Command: command,
	}
	rf.log = append(rf.log, log)
	fmt.Println(rf.log)
	for i := range rf.nextIndex {
		rf.nextIndex[i] = index
	}
	return index, term, true
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
	rf.mu.Lock()
	rf.license = Follower
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) VoteProcess() {
	if rf.killed() == false {
		rf.mu.Lock()
		rf.license = Candidate
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.timer.Reset(time.Duration(rf.voteBasicTime+rand.Int31()%200) * time.Millisecond)
		args := RequestVoteArgs{}
		reply := RequestVoteReply{}
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		//args.LastLogIndex = len(rf.log) - 1
		//args.LastLogTerm = rf.log[args.LastLogIndex].Term
		args.LastLogIndex = rf.commitIndex
		args.LastLogTerm = rf.log[args.LastLogIndex].Term
		rf.agreeCount.Store(1)
		rf.mu.Unlock()

		wg := sync.WaitGroup{}
		wg.Add(len(rf.peers) - 1)
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go rf.sendRequestVote(i, &args, &reply, &wg)
			}
		}
		wg.Wait()
		fmt.Printf("(%v): Term = %v, agreeCount = %v, sumCount = %v \n", rf.me, rf.currentTerm, rf.agreeCount.Load(), len(rf.peers))
	}
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

func (rf *Raft) ReceiveHeartBeatFromLeader(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.currentTerm = args.Term
	//fmt.Printf("(%v): receive message from leader(%v) --- args.term = %v currentTerm = %v commitIndex = %v\n", rf.me, args.LeaderId, args.Term, rf.currentTerm, args.LeaderCommit)
	rf.license = Follower
	if args.LeaderCommit > rf.commitIndex {
		preCommitIndex := rf.commitIndex
		//rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		fmt.Printf("(%v): MatchIndex : %v  leaderId : %v\n", rf.me, rf.matchIndex, args.LeaderId)
		rf.commitIndex = min(args.LeaderCommit, rf.matchIndex[args.LeaderId])
		rf.lastApplied = rf.commitIndex
		fmt.Printf("(%v) commitIndex (%v)  log: %v\n", rf.me, rf.commitIndex, rf.log)

		for i := preCommitIndex + 1; i <= rf.commitIndex; i++ {
			rf.sendCommitMsg2ApplyCh(ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			})
		}
	} else if args.LeaderCommit < rf.commitIndex {
		reply.Term = -1
	}
	reply.Success = true
	rf.timer.Reset(rf.heartBeatTime)
}
func (rf *Raft) initNextIndex(value int) {
	for i := range rf.peers {
		rf.nextIndex[i] = value
	}
}
func (rf *Raft) ReceiveLogCopyFromLeader(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.PrevLogIndex < rf.commitIndex {
		reply.Term = -1
	} else if args.PrevLogIndex >= len(rf.log) {
		return
	} else if rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
		reply.Success = true
		rf.log = append(rf.log[0:args.PrevLogIndex+1], args.Entries...)
		rf.matchIndex[args.LeaderId] = len(rf.log) - 1
		rf.initNextIndex(len(rf.log))
		fmt.Printf("(%v) : ReceiveLogCopyFromLeader (%v) success Log: %v\n", rf.me, args.LeaderId, rf.log)
	}
}

func (rf *Raft) ReceiveMsgFromLeader(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		fmt.Println(rf.currentTerm, args.Term)
		reply.Success = false
		return
	}
	if len(args.Entries) == 0 { //heartbeat
		rf.ReceiveHeartBeatFromLeader(args, reply)
	} else { //logCopy
		rf.ReceiveLogCopyFromLeader(args, reply)
	}
}

func (rf *Raft) HeartBeatProcess() {
	if rf.killed() == false {
		args := AppendEntriesArgs{}
		rf.mu.Lock()
		rf.timer.Reset(rf.heartBeatTime)
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.LeaderCommit = rf.commitIndex
		rf.mu.Unlock()

		wg := sync.WaitGroup{}
		wg.Add(len(rf.peers) - 1)
		for i := 0; i < len(rf.peers); i++ {
			reply := AppendEntriesReply{
				Term:    -1,
				Success: false,
			}
			if i != rf.me {
				go rf.sendHeartBeat(i, &args, &reply, &wg)
			}
		}
		wg.Wait()
	}
}
func (rf *Raft) logCopyReqProcess(index int) {
	if rf.killed() == false {
		var agreeNum int32 = 1
		var sumNum int32 = 1
		var flag atomic.Bool
		flag.Store(false)
		resChan := make(chan int)
		for i := range rf.peers {
			if i != rf.me {
				go rf.sendLogCopyRequest(i, &agreeNum, &sumNum, &flag, &resChan)
			}
		}
		res := <-resChan
		fmt.Printf("logCopyReqProcess : (%v) agreeCount: %v  sumCount: %v  res: %v\n", rf.me, agreeNum, len(rf.peers), res)
		if res == 1 {
			rf.mu.Lock()
			fmt.Println("index: ", index)
			rf.commitIndex = index
			preIndex := rf.lastApplied
			rf.mu.Unlock()
			go rf.HeartBeatProcess()
			for i := preIndex + 1; i <= index; i++ {
				rf.sendCommitMsg2ApplyCh(ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Command,
					CommandIndex: i,
				})
			}
			rf.mu.Lock()
			rf.lastApplied = max(rf.lastApplied, rf.commitIndex)
			rf.mu.Unlock()
		}
	}
}
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		select {
		case <-rf.timer.C:
			//即超时
			rf.mu.Lock()
			license := rf.license
			rf.mu.Unlock()
			switch license {
			case Follower:
				fallthrough
			case Candidate:
				go rf.VoteProcess()
				fmt.Printf("(%v) : currentTerm = %v  become candidate\n", rf.me, rf.currentTerm)
			case Leader:
				go rf.HeartBeatProcess()
				fmt.Printf("(%v) : currentTerm = %v  become leader\n", rf.me, rf.currentTerm)
			}
		}
	}
}

func (rf *Raft) sendCommitMsg2ApplyCh(msg ApplyMsg) {
	fmt.Printf("(%v) sendCommitMsg2ApplyCh  content: %v\n", rf.me, msg)
	*rf.applyMsgChan <- msg
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
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 1
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.license = Follower
	rf.log = append(rf.log, Log{
		Term:    0,
		Command: "start",
	})
	rf.heartBeatTime = time.Duration(100) * time.Millisecond
	rf.voteBasicTime = 250
	rf.timer = time.NewTimer(time.Duration(rf.voteBasicTime+rand.Int31()%200) * time.Millisecond)
	rf.applyMsgChan = &applyCh

	rf.mu.Unlock()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
