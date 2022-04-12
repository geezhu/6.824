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
	"6.824/labgob"
	"bytes"
	"math/rand"
	"time"
	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
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

//Log Entry Definenation
type Entry struct {
	Command     interface{}
	CommandTerm int
}

//raft's role
type Role int

const (
	Follower  Role = iota
	Candidate Role = iota
	Leader    Role = iota
)

//For VotedFor, No Candidate is voted for this raft instance
const null int = -1

type RWLock struct {
	rw sync.RWMutex
}

//these functions are used for debugging lock earlier
func (rw *RWLock) RLock() {
	rw.rw.RLock()
}
func (rw *RWLock) RUnlock() {
	rw.rw.RUnlock()
}
func (rw *RWLock) Lock() {
	rw.rw.Lock()
}
func (rw *RWLock) Unlock() {
	rw.rw.Unlock()
}

//basic state for raft
type basicState struct {
	rw          RWLock
	wg          sync.WaitGroup
	State       Role
	CurrentTerm int
	VotedFor    int
}

//record the Snapshot's info
type snapshotState struct {
	rw                RWLock
	LastIncludedTerm  int
	LastIncludedIndex int
}

//Log state, highestIndex is not necessary, for simplicity, we reserve it
type logstate struct {
	rw           RWLock
	BaseIndex    int
	HighestIndex int
	Log          []Entry
}

//Used For Counting Votes
type count struct {
	rw          RWLock
	VoteCount   int
	UnVoteCount int
}

//timer for detect timeout
type timerProperty struct {
	mu    sync.Mutex
	timer *time.Timer
	reset chan int
}

//the info about peers(Follower)
type peerIndex struct {
	rw         []RWLock
	nextIndex  []int
	matchIndex []int
}

//raft own Index
type myIndex struct {
	rw          RWLock
	CommitIndex int
	LastApplied int
}

//
// A Go object implementing a single Raft peer.
//

type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // this peer's index into peers[]
	dead            int32               // set by Kill()
	bstate          basicState
	lstate          logstate
	sstate          snapshotState
	mIndex          myIndex
	pIndex          peerIndex
	electionTimeout int
	vc              count
	tp              timerProperty
	applyCh         chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

type timeout int

//const int for timeout
const (
	LWRBound   timeout = 250
	UPRBound   timeout = 450
	HrtBt      timeout = 100
	ApplyINTVL timeout = 1
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.bstate.rw.RLock()
	defer rf.bstate.rw.RUnlock()
	term = rf.bstate.CurrentTerm
	isleader = rf.bstate.State == Leader
	return term, isleader
}
func (rf *Raft) IsLeader() bool {
	rf.bstate.rw.RLock()
	defer rf.bstate.rw.RUnlock()
	return rf.bstate.State == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	//pre:bstate.R,lstate.R,mIndex.R,sstate.R
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(rf.bstate.CurrentTerm)
	_ = e.Encode(rf.bstate.VotedFor)
	_ = e.Encode(rf.lstate.Log)
	_ = e.Encode(rf.lstate.BaseIndex)
	_ = e.Encode(rf.lstate.HighestIndex)
	_ = e.Encode(rf.sstate.LastIncludedTerm)
	_ = e.Encode(rf.sstate.LastIncludedIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// save Raft's persistent state and snapshot to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	//pre:bstate.R,lstate.R,mIndex.R,sstate.R
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(rf.bstate.CurrentTerm)
	_ = e.Encode(rf.bstate.VotedFor)
	_ = e.Encode(rf.lstate.Log)
	_ = e.Encode(rf.lstate.BaseIndex)
	_ = e.Encode(rf.lstate.HighestIndex)
	_ = e.Encode(rf.sstate.LastIncludedTerm)
	_ = e.Encode(rf.sstate.LastIncludedIndex)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var baseIndex int
	var highestIndex int
	var lastIncludedTerm int
	var lastIncludedIndex int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&rf.lstate.Log) != nil ||
		d.Decode(&baseIndex) != nil ||
		d.Decode(&highestIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&lastIncludedIndex) != nil {
		panic("readPersist:Decode Err!")
	} else {
		rf.bstate.CurrentTerm = currentTerm
		rf.bstate.VotedFor = votedFor
		rf.lstate.BaseIndex = baseIndex
		rf.lstate.HighestIndex = highestIndex
		rf.mIndex.CommitIndex = lastIncludedIndex
		rf.mIndex.LastApplied = lastIncludedIndex
		rf.sstate.LastIncludedIndex = lastIncludedIndex
		rf.sstate.LastIncludedTerm = lastIncludedTerm
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.bstate.rw.RLock()
	rf.mIndex.rw.Lock()
	rf.lstate.rw.Lock()
	rf.sstate.rw.Lock()
	defer rf.sstate.rw.Unlock()
	defer rf.lstate.rw.Unlock()
	defer rf.mIndex.rw.Unlock()
	defer rf.bstate.rw.RUnlock()
	if lastIncludedIndex < rf.sstate.LastIncludedIndex || lastIncludedTerm < rf.sstate.LastIncludedTerm {
		return false
	} else if lastIncludedIndex < rf.mIndex.CommitIndex {
		return false
	}
	rf.sstate.LastIncludedTerm = lastIncludedTerm
	rf.sstate.LastIncludedIndex = lastIncludedIndex
	if lastIncludedIndex >= rf.lstate.BaseIndex && lastIncludedIndex <= rf.ServerHighestEntryIndex() {
		rf.ServerTrimEntry(rf.lstate.BaseIndex, lastIncludedIndex)
	} else if lastIncludedIndex < rf.lstate.BaseIndex-1 {
		panic("snapshot too small")
	}
	if rf.mIndex.CommitIndex < lastIncludedIndex {
		rf.mIndex.CommitIndex = lastIncludedIndex
	}
	if rf.mIndex.LastApplied < lastIncludedIndex {
		rf.mIndex.LastApplied = lastIncludedIndex
	}
	if lastIncludedIndex > rf.ServerHighestEntryIndex() {
		rf.lstate.HighestIndex = lastIncludedIndex
		rf.lstate.BaseIndex = lastIncludedIndex + 1
		rf.lstate.Log = make([]Entry, 0)
	}
	rf.persistStateAndSnapshot(snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.bstate.rw.RLock()
	rf.mIndex.rw.RLock()
	rf.lstate.rw.Lock()
	rf.sstate.rw.Lock()
	defer rf.sstate.rw.Unlock()
	defer rf.lstate.rw.Unlock()
	defer rf.mIndex.rw.RUnlock()
	defer rf.bstate.rw.RUnlock()
	rf.sstate.LastIncludedTerm = rf.lstate.Log[rf.ServerIndexTransformer(index)].CommandTerm
	rf.sstate.LastIncludedIndex = index
	if rf.mIndex.CommitIndex < index {
		rf.mIndex.CommitIndex = index
	}
	if rf.mIndex.LastApplied < index {
		rf.mIndex.LastApplied = index
	}
	rf.ServerTrimEntry(rf.lstate.BaseIndex, index)
	if rf.lstate.BaseIndex <= rf.sstate.LastIncludedIndex {
		rf.lstate.BaseIndex = rf.sstate.LastIncludedIndex + 1
		rf.lstate.HighestIndex = rf.lstate.BaseIndex + len(rf.lstate.Log) - 1
	}
	rf.persistStateAndSnapshot(snapshot)
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
	rf.bstate.rw.RLock()
	defer rf.bstate.rw.RUnlock()
	term = rf.bstate.CurrentTerm
	isLeader := rf.bstate.State == Leader
	if !isLeader {
		return index, term, isLeader
	}
	rf.lstate.rw.Lock()
	defer rf.lstate.rw.Unlock()
	e := Entry{
		Command:     command,
		CommandTerm: rf.bstate.CurrentTerm,
	}
	rf.lstate.Log = append(rf.lstate.Log, e)
	rf.lstate.HighestIndex += 1
	index = rf.lstate.HighestIndex
	rf.pIndex.rw[rf.me].rw.Lock()
	rf.pIndex.matchIndex[rf.me] = index
	rf.pIndex.nextIndex[rf.me] = index + 1
	rf.pIndex.rw[rf.me].rw.Unlock()
	rf.sstate.rw.RLock()
	defer rf.sstate.rw.RUnlock()
	rf.persist()
	go rf.LdrSendAE()

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
	rf.mu.Lock()
	rf.tp.timer = time.NewTimer(time.Duration(rf.electionTimeout) * time.Millisecond)
	defer rf.tp.timer.Stop()
	rf.mu.Unlock()
	for rf.killed() == false {
		rf.ServerResetTimer()
		rf.ServerWaitToWakeup()
		rf.bstate.rw.Lock()
		switch rf.bstate.State {
		case Leader:
			rf.LdrTicker()
		case Follower:
			rf.FollowerTicker()
		case Candidate:
			rf.CandidateTicker()
		default:
		}
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
	}
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
	rf.bstate.State = Follower
	rf.bstate.VotedFor = null
	rf.bstate.CurrentTerm = 0
	rf.mIndex.CommitIndex = 0
	rf.mIndex.LastApplied = 0
	rf.pIndex.nextIndex = make([]int, len(peers))

	for i := 0; i < len(peers); i++ {
		rf.pIndex.nextIndex[i] = 1
	}
	rf.pIndex.matchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.pIndex.matchIndex[i] = 0
	}

	rf.pIndex.rw = make([]RWLock, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.pIndex.rw[i] = RWLock{}
	}
	rf.lstate.Log = make([]Entry, 0)
	rf.lstate.BaseIndex = 1
	rf.lstate.HighestIndex = 0
	rf.electionTimeout = rf.ServerGetRandomTimeout(LWRBound, UPRBound)
	rf.vc.VoteCount = 0
	rf.vc.UnVoteCount = 0
	rf.tp.reset = make(chan int)
	rf.sstate.LastIncludedIndex = 0
	rf.sstate.LastIncludedTerm = 0
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	s := ApplyMsg{
		CommandValid:  false,
		Command:       nil,
		CommandIndex:  0,
		SnapshotValid: true,
		Snapshot:      persister.ReadSnapshot(),
		SnapshotTerm:  rf.sstate.LastIncludedTerm,
		SnapshotIndex: rf.sstate.LastIncludedIndex,
	}

	//If commitIndex > lastApplied: increment lastApplied, apply
	//log[lastApplied] to state machine (§5.3)
	applyEntry := func() {
		if s.Snapshot != nil && s.SnapshotIndex != 0 {
			rf.applyCh <- s
		}
		for rf.killed() == false {
			rf.mIndex.rw.Lock()
			for rf.mIndex.CommitIndex > rf.mIndex.LastApplied {
				// apply entry to state machine
				rf.lstate.rw.RLock()
				e := rf.lstate.Log[rf.ServerIndexTransformer(rf.mIndex.LastApplied+1)]
				rf.lstate.rw.RUnlock()
				AP := ApplyMsg{
					CommandValid:  true,
					Command:       e.Command,
					CommandIndex:  rf.mIndex.LastApplied + 1,
					SnapshotValid: false,
					Snapshot:      nil,
					SnapshotTerm:  0,
					SnapshotIndex: 0,
				}
				next := rf.mIndex.LastApplied + 1
				rf.mIndex.rw.Unlock()
				applyCh <- AP
				rf.mIndex.rw.Lock()
				if next > rf.mIndex.LastApplied {
					rf.mIndex.LastApplied = next
				}

			}
			rf.mIndex.rw.Unlock()
			time.Sleep(time.Duration(ApplyINTVL) * time.Millisecond)
		}
	}
	go applyEntry()
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

//RPC

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//XHighestIndex
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.bstate.rw.RLock()
	switch rf.bstate.State {
	case Leader:
		rf.bstate.rw.RUnlock()
		rf.LdrRecvRV(args, reply)
	case Follower:
		rf.bstate.rw.RUnlock()
		rf.FollowerRecvRV(args, reply)
	case Candidate:
		rf.bstate.rw.RUnlock()
		rf.CandidateRecvRV(args, reply)
	default:
	}
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

//
// example InstallSnapshot RPC arguments structure.
// field names must start with capital letters!
//
type InstallSnapshotArgs struct {
	// Your data here (2D).
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

//
// example InstallSnapshot RPC reply structure.
// field names must start with capital letters!
//
type InstallSnapshotReply struct {
	// Your data here (2D).
	Term int
}

//
// example InstallSnapshot RPC handler.
//
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// Your code here (2D).
	rf.bstate.rw.Lock()
	reply.Term = rf.bstate.CurrentTerm
	if args.Term < rf.bstate.CurrentTerm {
		rf.bstate.rw.Unlock()
		return
	}
	if args.Term > rf.bstate.CurrentTerm {
		rf.vc.rw.Lock()
		defer rf.vc.rw.Unlock()
		rf.ServerRecvLargerTerm(args.Term)
		if rf.bstate.VotedFor == null {
			rf.ServerGrantVote(args.LeaderId)
		}
		rf.lstate.rw.RLock()
		rf.sstate.rw.RLock()
		rf.persist()
		rf.sstate.rw.RUnlock()
		rf.lstate.rw.RUnlock()
	}
	rf.bstate.rw.Unlock()
	s := ApplyMsg{
		CommandValid:  false,
		Command:       nil,
		CommandIndex:  0,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	if s.Snapshot != nil && s.SnapshotIndex != 0 {
		rf.applyCh <- s
	} else {
		panic("Wrong Snapshot")
	}
	rf.bstate.rw.RLock()
	defer rf.bstate.rw.RUnlock()
	reply.Term = rf.bstate.CurrentTerm
}

//
// example code to send a InstallSnapshot RPC to a server.
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
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term          int
	Success       bool
	XTerm         int
	XIndex        int
	XHighestIndex int
}

//
// example AppendEntries RPC handler.
//HeartBeart
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.bstate.rw.RLock()
	switch rf.bstate.State {
	case Leader:
		rf.bstate.rw.RUnlock()
		rf.LdrRecvAE(args, reply)
	case Follower:
		rf.bstate.rw.RUnlock()
		rf.FollowerRecvAE(args, reply)
	case Candidate:
		rf.bstate.rw.RUnlock()
		rf.CandidateRecvAE(args, reply)
	default:
	}
}

//
// example code to send a AppendEntries RPC to a server.
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
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) LdrRecvAE(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//pre: nothing
	//cur:bstate.L,lstate.L
	rf.bstate.rw.Lock()
	defer rf.bstate.rw.Unlock()
	if args.Term < rf.bstate.CurrentTerm {
		reply.Term = rf.bstate.CurrentTerm
		reply.Success = false
		reply.XTerm = 0
		reply.XIndex = 0
		reply.XHighestIndex = rf.ServerHighestEntryIndex()
		return
	}
	if args.Term == rf.bstate.CurrentTerm {
		if args.LeaderId != rf.me {
			reply.Term = rf.bstate.CurrentTerm
			reply.Success = false
		} else {
			reply.Term = rf.bstate.CurrentTerm
			reply.Success = true
		}
		reply.XTerm = 0
		reply.XIndex = 0
		reply.XHighestIndex = rf.ServerHighestEntryIndex()
		return
	}
	if args.Term > rf.bstate.CurrentTerm {
		if rf.bstate.VotedFor != args.LeaderId && rf.bstate.VotedFor != null {
			rf.vc.rw.Lock()
			rf.ServerRecvLargerTerm(args.Term)
			rf.vc.rw.Unlock()
			reply.XTerm = 0
			reply.XIndex = 0
			reply.XHighestIndex = rf.ServerHighestEntryIndex()
			reply.Term = rf.bstate.CurrentTerm
			reply.Success = false
			rf.lstate.rw.RLock()
			rf.sstate.rw.RLock()
			rf.persist()
			rf.sstate.rw.RUnlock()
			rf.lstate.rw.RUnlock()
			return
		}
		rf.vc.rw.Lock()
		rf.ServerRecvLargerTerm(args.Term)
		rf.vc.rw.Unlock()

		rf.ServerResetTimer()

		rf.lstate.rw.RLock()
		rf.sstate.rw.RLock()
		rf.persist()
		rf.sstate.rw.RUnlock()
		rf.lstate.rw.RUnlock()
	}
	rf.mIndex.rw.Lock()
	defer rf.mIndex.rw.Unlock()
	rf.lstate.rw.Lock()
	defer rf.lstate.rw.Unlock()
	if !rf.ServerHasLogEntry(args.PrevLogIndex, args.PrevLogTerm) {
		rf.ServerRemoveConflictingEntry(args.PrevLogIndex + 1)
		rf.sstate.rw.RLock()
		rf.persist()
		rf.sstate.rw.RUnlock()
		reply.Term = rf.bstate.CurrentTerm
		reply.Success = false
		reply.XHighestIndex = rf.ServerHighestEntryIndex()
		if reply.XHighestIndex >= args.PrevLogIndex {
			reply.XTerm = rf.lstate.Log[rf.ServerIndexTransformer(args.PrevLogIndex)].CommandTerm
			reply.XIndex = rf.ServerFastLogFronttracking(args.PrevLogIndex)
		} else {
			reply.XTerm = 0
			reply.XIndex = 0
		}
		return
	} else if len(args.Entries) > 0 {
		rf.ServerAppendEntries(args.PrevLogIndex+1, args.Entries)
	} else if len(args.Entries) == 0 {
		//nothing to do
	}
	reply.XTerm = 0
	reply.XIndex = 0
	reply.XHighestIndex = rf.ServerHighestEntryIndex()
	reply.Term = rf.bstate.CurrentTerm
	reply.Success = true
	rf.ServerUpdateCommitIndex(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
}
func (rf *Raft) LdrRecvRV(args *RequestVoteArgs, reply *RequestVoteReply) {
	//pre:
	//cur:bstate.L,lstate.R
	rf.bstate.rw.Lock()
	defer rf.bstate.rw.Unlock()
	if args.Term <= rf.bstate.CurrentTerm { //eq is not acceptable for a leader
		reply.Term = rf.bstate.CurrentTerm
		reply.VoteGranted = false
		return
	}
	rf.lstate.rw.RLock()
	logIsCompatiable := rf.ServerCheckSenderLog(args.LastLogIndex, args.LastLogTerm)
	rf.lstate.rw.RUnlock()
	if args.Term > rf.bstate.CurrentTerm {
		rf.vc.rw.Lock()
		rf.ServerRecvLargerTerm(args.Term)
		rf.vc.rw.Unlock()
		rf.lstate.rw.RLock()
		rf.sstate.rw.RLock()
		rf.persist()
		rf.sstate.rw.RUnlock()
		rf.lstate.rw.RUnlock()
	}
	if (rf.bstate.VotedFor == null || rf.bstate.VotedFor == args.CandidateId) && logIsCompatiable {
		rf.ServerGrantVote(args.CandidateId)
		reply.Term = rf.bstate.CurrentTerm
		reply.VoteGranted = true
	} else {
		reply.Term = rf.bstate.CurrentTerm
		reply.VoteGranted = false
	}
}
func (rf *Raft) LdrSendAE() {
	//pre:
	//cur:
	rf.lstate.rw.RLock()
	highestIndex := rf.ServerHighestEntryIndex()
	rf.lstate.rw.RUnlock()
	for peer := range rf.peers {
		rf.pIndex.rw[peer].RLock()
		if rf.pIndex.nextIndex[peer] <= rf.sstate.LastIncludedIndex {
			go rf.LdrSendSnapshot(peer)
		} else if highestIndex >= rf.pIndex.nextIndex[peer] {
			go rf.LdrSendCmd(peer)
		} else {
			go rf.LdrSendHB(peer)
		}
		rf.pIndex.rw[peer].RUnlock()
	}
}
func (rf *Raft) LdrSendHB(peer int) {
	//pre:nothing but(pIndex.R) maybe racing.
	//cur:bstate.R,mIndex.R,bstate.L,pIndex.L,lstate.R
	var args AppendEntriesArgs
	var reply AppendEntriesReply

	rf.bstate.rw.RLock()
	if rf.bstate.State != Leader {
		rf.bstate.rw.RUnlock()
		return
	}
	rf.mIndex.rw.RLock()
	oldTerm := rf.bstate.CurrentTerm
	args.Term = rf.bstate.CurrentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.mIndex.CommitIndex
	rf.mIndex.rw.RUnlock()
	rf.bstate.rw.RUnlock()
	args.Entries = make([]Entry, 0)
	rf.lstate.rw.RLock()
	rf.pIndex.rw[peer].RLock()
	args.PrevLogIndex = rf.ServerPrevLogIndex(peer)
	if args.PrevLogIndex+1 < rf.lstate.BaseIndex {
		go rf.LdrSendSnapshot(peer)
		rf.pIndex.rw[peer].RUnlock()
		rf.lstate.rw.RUnlock()
		return
	}
	args.PrevLogTerm = rf.ServerPrevLogTerm(peer)
	rf.pIndex.rw[peer].RUnlock()
	rf.lstate.rw.RUnlock()
	if rf.sendAppendEntries(peer, &args, &reply) != true {
		return
	}

	rf.bstate.rw.Lock()
	defer rf.bstate.rw.Unlock()
	if rf.bstate.State != Leader {
		return
	} else if oldTerm != rf.bstate.CurrentTerm && reply.Term <= rf.bstate.CurrentTerm {
		return
	}

	if reply.Term > rf.bstate.CurrentTerm {
		rf.vc.rw.Lock()
		rf.ServerRecvLargerTerm(reply.Term)
		rf.vc.rw.Unlock()
		rf.lstate.rw.RLock()
		rf.sstate.rw.RLock()
		rf.persist()
		rf.sstate.rw.RUnlock()
		rf.lstate.rw.RUnlock()
		//可能有潜在的并发风险，即一个Term被多次recv，将可能影响VotedFor
	} else if reply.Success == false {
		//If AppendEntries fails because of log inconsistency:
		//decrement nextIndex and retry
		rf.lstate.rw.RLock()
		defer rf.lstate.rw.RUnlock()
		rf.pIndex.rw[peer].Lock()
		defer rf.pIndex.rw[peer].Unlock()
		//乱序风险
		if reply.XHighestIndex < rf.pIndex.matchIndex[peer] {
			return
		}
		if reply.XHighestIndex < rf.pIndex.nextIndex[peer]-1 {
			rf.pIndex.nextIndex[peer] = reply.XHighestIndex + 1
		} else if res, index := rf.ServerContainsTerm(reply.XTerm); res {
			rf.pIndex.nextIndex[peer] = index
		} else if reply.XTerm != 0 {
			rf.pIndex.nextIndex[peer] = reply.XIndex
		}
	} else if reply.Success {
		//If successful: update nextIndex and matchIndex for
		//follower
		//for HeartBeat it contains all the entry before nextIndex[peer]
		//so we update matchIndex ,the highest Index in Follower's Log
		rf.pIndex.rw[peer].Lock()

		if i := args.PrevLogIndex + len(args.Entries); i > rf.pIndex.matchIndex[peer] {
			if i > rf.lstate.HighestIndex {
				panic("LdrSendCmd: i>rf.lstate.HighestIndex")
			}
			rf.pIndex.matchIndex[peer] = i //never send anything
			rf.pIndex.rw[peer].Unlock()
			npeer := len(rf.peers)
			count := 0
			rf.mIndex.rw.Lock()
			rf.lstate.rw.RLock()
			if i < rf.lstate.BaseIndex {
				rf.lstate.rw.RUnlock()
				rf.mIndex.rw.Unlock()
				return
			}
			for j := 0; j < npeer; j++ {
				rf.pIndex.rw[j].RLock()
				if rf.pIndex.matchIndex[j] >= i {
					count += 1
				}
				rf.pIndex.rw[j].RUnlock()
			}
			if count >= npeer/2+1 && i >= rf.lstate.BaseIndex && rf.lstate.Log[rf.ServerIndexTransformer(i)].CommandTerm == rf.bstate.CurrentTerm && i > rf.mIndex.CommitIndex {
				rf.mIndex.CommitIndex = i
				rf.sstate.rw.RLock()
				rf.persist()
				rf.sstate.rw.RUnlock()
			}
			rf.lstate.rw.RUnlock()
			rf.mIndex.rw.Unlock()
		} else {
			rf.pIndex.rw[peer].Unlock()
		}
	}
}

//Leader send entries to Follower
func (rf *Raft) LdrSendCmd(peer int) {
	//pre:nothing but(pIndex.R) maybe racing.
	//cur:bstate.R,mIndex.R,bstate.L,pIndex.L,lstate.R
	var args AppendEntriesArgs
	var reply AppendEntriesReply
	rf.bstate.rw.RLock()
	if rf.bstate.State != Leader {
		rf.bstate.rw.RUnlock()
		return
	}

	rf.mIndex.rw.RLock()

	oldTerm := rf.bstate.CurrentTerm
	args.Term = rf.bstate.CurrentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.mIndex.CommitIndex

	rf.mIndex.rw.RUnlock()
	rf.bstate.rw.RUnlock()

	rf.lstate.rw.RLock()
	rf.pIndex.rw[peer].RLock()

	low := rf.ServerIndexTransformer(rf.pIndex.nextIndex[peer])
	high := rf.ServerIndexTransformer(rf.ServerHighestEntryIndex())
	if low < 0 {
		go rf.LdrSendSnapshot(peer)
		rf.pIndex.rw[peer].RUnlock()
		rf.lstate.rw.RUnlock()
		return
	}
	args.Entries = rf.ServerReadSlice(low, high)
	args.PrevLogIndex = rf.ServerPrevLogIndex(peer)
	args.PrevLogTerm = rf.ServerPrevLogTerm(peer)

	rf.pIndex.rw[peer].RUnlock()
	rf.lstate.rw.RUnlock()

	if rf.sendAppendEntries(peer, &args, &reply) != true {
		return
	}

	rf.bstate.rw.Lock()
	defer rf.bstate.rw.Unlock()
	if rf.bstate.State != Leader {
		return
	} else if oldTerm != rf.bstate.CurrentTerm && reply.Term <= rf.bstate.CurrentTerm {
		return
	}
	if reply.Term > rf.bstate.CurrentTerm {
		rf.vc.rw.Lock()
		defer rf.vc.rw.Unlock()
		rf.ServerRecvLargerTerm(reply.Term)
		rf.lstate.rw.RLock()
		rf.sstate.rw.RLock()
		rf.persist()
		rf.sstate.rw.RUnlock()
		rf.lstate.rw.RUnlock()
		//可能有潜在的并发风险，即一个Term被多次recv，将可能影响VotedFor
	} else if reply.Success == false {
		//If AppendEntries fails because of log inconsistency:
		//decrement nextIndex and retry
		rf.lstate.rw.RLock()
		defer rf.lstate.rw.RUnlock()
		rf.pIndex.rw[peer].Lock()
		defer rf.pIndex.rw[peer].Unlock()
		if reply.XHighestIndex < rf.pIndex.matchIndex[peer] {
			return
		}
		if reply.XHighestIndex < rf.pIndex.nextIndex[peer]-1 {
			rf.pIndex.nextIndex[peer] = reply.XHighestIndex + 1
		} else if res, index := rf.ServerContainsTerm(reply.XTerm); res {
			rf.pIndex.nextIndex[peer] = index
		} else if reply.XTerm != 0 {
			rf.pIndex.nextIndex[peer] = reply.XIndex
		}
	} else if reply.Success {
		//If successful: update nextIndex and matchIndex for
		//follower
		//for HeartBeat it contains all the entry before nextIndex[peer]
		//so we update matchIndex ,the highest Index in Follower's Log
		rf.pIndex.rw[peer].Lock()
		if i := args.PrevLogIndex + len(args.Entries); i > rf.pIndex.matchIndex[peer] {
			if i > rf.lstate.HighestIndex {
				panic("LdrSendCmd: i>rf.lstate.HighestIndex")
			}
			rf.pIndex.matchIndex[peer] = i //never send anything
			rf.pIndex.nextIndex[peer] = rf.pIndex.matchIndex[peer] + 1
			rf.pIndex.rw[peer].Unlock()
			npeer := len(rf.peers)
			count := 0
			rf.mIndex.rw.Lock()
			rf.lstate.rw.RLock()
			for j := 0; j < npeer; j++ {
				rf.pIndex.rw[j].RLock()
				if rf.pIndex.matchIndex[j] >= i {
					count += 1
				}
				rf.pIndex.rw[j].RUnlock()
			}
			if count >= npeer/2+1 && i >= rf.lstate.BaseIndex && rf.lstate.Log[rf.ServerIndexTransformer(i)].CommandTerm == rf.bstate.CurrentTerm && i > rf.mIndex.CommitIndex {
				rf.mIndex.CommitIndex = i
				rf.sstate.rw.RLock()
				rf.persist()
				rf.sstate.rw.RUnlock()
			}
			rf.lstate.rw.RUnlock()
			rf.mIndex.rw.Unlock()
		} else {
			rf.pIndex.rw[peer].Unlock()
		}

	}
}
func (rf *Raft) LdrSendSnapshot(peer int) {
	rf.bstate.rw.RLock()
	rf.sstate.rw.RLock()
	OldTerm := rf.bstate.CurrentTerm
	var args = InstallSnapshotArgs{
		Term:              rf.bstate.CurrentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.sstate.LastIncludedIndex,
		LastIncludedTerm:  rf.sstate.LastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.sstate.rw.RUnlock()
	rf.bstate.rw.RUnlock()
	var reply InstallSnapshotReply
	if rf.sendInstallSnapshot(peer, &args, &reply) != true {
		return
	}
	rf.bstate.rw.Lock()
	rf.vc.rw.Lock()
	rf.pIndex.rw[peer].Lock()
	if i := rf.sstate.LastIncludedIndex; i > rf.pIndex.matchIndex[peer] && rf.bstate.State == Leader && OldTerm == rf.bstate.CurrentTerm {
		if i > rf.lstate.HighestIndex {
			panic("LdrSendCmd: i>rf.lstate.HighestIndex")
		}
		rf.pIndex.matchIndex[peer] = i //never send anything
		rf.pIndex.nextIndex[peer] = rf.pIndex.matchIndex[peer] + 1
	}
	rf.pIndex.rw[peer].Unlock()
	defer rf.vc.rw.Unlock()
	defer rf.bstate.rw.Unlock()
	if reply.Term > rf.bstate.CurrentTerm {
		rf.ServerRecvLargerTerm(reply.Term)
		rf.lstate.rw.RLock()
		rf.sstate.rw.RLock()
		rf.persist()
		rf.sstate.rw.RUnlock()
		rf.lstate.rw.RUnlock()
	}
}
func (rf *Raft) LdrTicker() {
	//pre:bstate.L
	//cur:
	//nothing to do
	rf.bstate.rw.Unlock()
}
func (rf *Raft) CandidateRecvAE(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//pre:nothing
	//cur:bstate.L,lstate.L
	rf.bstate.rw.Lock()
	rf.lstate.rw.RLock()
	rf.lstate.rw.RUnlock()
	defer rf.bstate.rw.Unlock()
	if args.Term < rf.bstate.CurrentTerm {
		reply.Term = rf.bstate.CurrentTerm
		reply.Success = false
		reply.XTerm = 0
		reply.XIndex = 0
		reply.XHighestIndex = rf.ServerHighestEntryIndex()
		return
	}
	rf.vc.rw.Lock()
	defer rf.vc.rw.Unlock()
	if args.Term == rf.bstate.CurrentTerm {
		rf.CandidateToFollower(args.Term)
		rf.ServerResetTimer()
		rf.lstate.rw.RLock()
		rf.sstate.rw.RLock()
		rf.persist()
		rf.sstate.rw.RUnlock()
		rf.lstate.rw.RUnlock()
	} else if args.Term > rf.bstate.CurrentTerm {
		rf.ServerRecvLargerTerm(args.Term)
		rf.ServerResetTimer()
		rf.lstate.rw.RLock()
		rf.sstate.rw.RLock()
		rf.persist()
		rf.sstate.rw.RUnlock()
		rf.lstate.rw.RUnlock()
	}
	rf.mIndex.rw.Lock()
	defer rf.mIndex.rw.Unlock()
	rf.lstate.rw.Lock()
	defer rf.lstate.rw.Unlock()
	if !rf.ServerHasLogEntry(args.PrevLogIndex, args.PrevLogTerm) {
		rf.ServerRemoveConflictingEntry(args.PrevLogIndex + 1)
		rf.sstate.rw.RLock()
		rf.persist()
		rf.sstate.rw.RUnlock()
		reply.Term = rf.bstate.CurrentTerm
		reply.Success = false
		reply.XHighestIndex = rf.ServerHighestEntryIndex()
		if reply.XHighestIndex >= args.PrevLogIndex {
			reply.XTerm = rf.lstate.Log[rf.ServerIndexTransformer(args.PrevLogIndex)].CommandTerm
			reply.XIndex = rf.ServerFastLogFronttracking(args.PrevLogIndex)
		} else {
			reply.XTerm = 0
			reply.XIndex = 0
		}
		return
	} else if len(args.Entries) > 0 {
		rf.ServerAppendEntries(args.PrevLogIndex+1, args.Entries)
	} else if len(args.Entries) == 0 {
		//nothing to do
	}
	reply.XTerm = 0
	reply.XIndex = 0
	reply.XHighestIndex = rf.ServerHighestEntryIndex()
	reply.Term = rf.bstate.CurrentTerm
	reply.Success = true
	rf.ServerUpdateCommitIndex(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
}
func (rf *Raft) CandidateRecvRV(args *RequestVoteArgs, reply *RequestVoteReply) {
	//pre:
	//cur:bstate.L,lstate.R
	rf.bstate.rw.Lock()
	rf.lstate.rw.RLock()
	rf.lstate.rw.RUnlock()
	defer rf.bstate.rw.Unlock()
	if args.Term < rf.bstate.CurrentTerm { //eq is not acceptable for a leader
		reply.Term = rf.bstate.CurrentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term == rf.bstate.CurrentTerm && args.CandidateId != rf.me {
		reply.Term = rf.bstate.CurrentTerm
		reply.VoteGranted = false
		return
	} else if args.Term == rf.bstate.CurrentTerm && args.CandidateId == rf.me {
		reply.Term = rf.bstate.CurrentTerm
		reply.VoteGranted = true
		rf.bstate.VotedFor = args.CandidateId
		return
	}
	rf.lstate.rw.RLock()
	logIsCompatiable := rf.ServerCheckSenderLog(args.LastLogIndex, args.LastLogTerm)
	rf.lstate.rw.RUnlock()
	if args.Term > rf.bstate.CurrentTerm {
		rf.vc.rw.Lock()
		defer rf.vc.rw.Unlock()
		rf.ServerRecvLargerTerm(args.Term)
		rf.lstate.rw.RLock()
		rf.sstate.rw.RLock()
		rf.persist()
		rf.sstate.rw.RUnlock()
		rf.lstate.rw.RUnlock()
	}
	if (rf.bstate.VotedFor == null || rf.bstate.VotedFor == args.CandidateId) && logIsCompatiable {
		rf.ServerGrantVote(args.CandidateId)
		reply.Term = rf.bstate.CurrentTerm
		reply.VoteGranted = true
	} else {
		reply.Term = rf.bstate.CurrentTerm
		reply.VoteGranted = false
	}
}
func (rf *Raft) CandidateSendRV(server int, args *RequestVoteArgs) {
	//pre:maybe racing with bstate.R in ServerToCandidate，but STC will exit,so it's okay
	//cur:bstate.R,vc.L,bstate.L,vc.R
	var reply RequestVoteReply
	rf.bstate.rw.RLock()
	rf.lstate.rw.RLock()
	rf.lstate.rw.RUnlock()
	oldTerm := rf.bstate.CurrentTerm
	rf.bstate.rw.RUnlock()
	rf.bstate.wg.Done()

	if rf.sendRequestVote(server, args, &reply) != true {
		return
	}
	rf.bstate.rw.Lock()
	if reply.Term > rf.bstate.CurrentTerm {
		rf.ServerRecvLargerTerm(reply.Term)
		rf.lstate.rw.RLock()
		rf.sstate.rw.RLock()
		rf.persist()
		rf.sstate.rw.RUnlock()
		rf.lstate.rw.RUnlock()
		rf.bstate.rw.Unlock()
		return
	}
	if oldTerm != rf.bstate.CurrentTerm {
		rf.bstate.rw.Unlock()
		return
	} else if rf.bstate.State != Candidate {
		rf.bstate.rw.Unlock()
		return
	}
	rf.vc.rw.Lock()
	if server != rf.me {
		if reply.VoteGranted {
			rf.vc.VoteCount += 1
		} else {
			rf.vc.UnVoteCount += 1
		}
	} else {
		//nothing to do
	}

	Majority := func() int {
		return len(rf.peers)/2 + 1
	}
	if rf.vc.VoteCount == Majority() && rf.bstate.State != Leader {
		rf.CandidateToLdr()
	}
	rf.vc.rw.Unlock()
	rf.bstate.rw.Unlock()
}
func (rf *Raft) CandidateToCandidate() {
	//pre:bstate.L
	//cur:
	rf.ServerToCandidate()
}
func (rf *Raft) CandidateToFollower(term int) {
	//this is used for the same Term while recv Ldr's AE
	//pre:bstate.L,vc.L
	//cur:
	rf.bstate.CurrentTerm = term
	rf.ServerToFollower()
}
func (rf *Raft) CandidateToLdr() {
	//pre:bstate.L
	//cur:pIndex.L,mIndex.L
	//send Heartbeat(if there is any new Entry send it,100ms)
	//reset the matchIndex and nextIndex for all the servers
	//send a no-op to servers
	//create a routine to commit log entry
	rf.bstate.State = Leader
	//for each server, index of highest log entry
	//known to be replicated on server
	//(initialized to 0, increases monotonically)
	//for each server, index of the next log entry
	//to send to that server (initialized to leader
	//last log index + 1)
	var wg sync.WaitGroup
	wg.Add(len(rf.pIndex.matchIndex))
	rf.lstate.rw.RLock()
	lastLogIndex := rf.ServerHighestEntryIndex()
	rf.lstate.rw.RUnlock()
	setPeerIndex := func(index int) {
		rf.pIndex.rw[index].Lock()
		rf.pIndex.matchIndex[index] = 0
		rf.pIndex.nextIndex[index] = lastLogIndex + 1
		rf.pIndex.rw[index].Unlock()
		wg.Done()
	}
	for i := 0; i < len(rf.peers); i++ {
		go setPeerIndex(i)
	}
	wg.Wait()
	rf.pIndex.rw[rf.me].Lock()
	rf.pIndex.matchIndex[rf.me] = lastLogIndex
	rf.pIndex.nextIndex[rf.me] = lastLogIndex + 1
	rf.pIndex.rw[rf.me].Unlock()
	//First, a leader must have the latest information on
	//which entries are committed. The Leader Completeness
	//Property guarantees that a leader has all committed entries,
	//but at the start of its term, it may not know which
	//those are. To find out, it needs to commit an entry from
	//its term. Raft handles this by having each leader commit
	//a blank no-op entry into the log at the start of its
	//term.
	addNoOpEntry := func() {
		noop := Entry{
			Command:     nil,
			CommandTerm: rf.bstate.CurrentTerm,
		}
		slice1 := make([]Entry, 0)
		slice1 = append(slice1, noop)
		rf.ServerAppendEntries(rf.ServerHighestEntryIndex()+1, slice1)
	}
	go addNoOpEntry()
	sendAE := func() {
		rf.bstate.rw.RLock()
		for rf.bstate.State == Leader && rf.killed() == false {
			rf.bstate.rw.RUnlock()
			go rf.LdrSendAE()
			time.Sleep(time.Duration(HrtBt) * time.Millisecond)
			rf.bstate.rw.RLock()
		}
		rf.bstate.rw.RUnlock()
	}
	go sendAE()
}
func (rf *Raft) CandidateTicker() {
	//pre:bstate.L
	//cur:
	rf.CandidateToCandidate()
}
func (rf *Raft) FollowerRecvAE(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//pre: nothing
	//cur:bstate.L,lstate.L
	rf.bstate.rw.Lock()
	rf.lstate.rw.RLock()
	rf.lstate.rw.RUnlock()
	defer rf.bstate.rw.Unlock()
	if args.Term < rf.bstate.CurrentTerm {
		reply.Term = rf.bstate.CurrentTerm
		reply.Success = false
		reply.XTerm = 0
		reply.XIndex = 0
		reply.XHighestIndex = rf.ServerHighestEntryIndex()
		return
	}
	if args.Term == rf.bstate.CurrentTerm {
		//nothing to do,just recv all the entry
		if args.LeaderId != rf.bstate.VotedFor && rf.bstate.VotedFor != null {
			reply.Term = rf.bstate.CurrentTerm
			reply.Success = false
			reply.XTerm = 0
			reply.XIndex = 0
			reply.XHighestIndex = rf.ServerHighestEntryIndex()
			return
		}
		rf.ServerResetTimer()
	} else if args.Term > rf.bstate.CurrentTerm {
		rf.vc.rw.Lock()
		defer rf.vc.rw.Unlock()
		rf.ServerRecvLargerTerm(args.Term)
		rf.ServerResetTimer()

		rf.lstate.rw.RLock()
		rf.sstate.rw.RLock()
		rf.persist()
		rf.sstate.rw.RUnlock()
		rf.lstate.rw.RUnlock()
	}
	rf.mIndex.rw.Lock()
	defer rf.mIndex.rw.Unlock()
	rf.lstate.rw.Lock()
	defer rf.lstate.rw.Unlock()
	if !rf.ServerHasLogEntry(args.PrevLogIndex, args.PrevLogTerm) {
		rf.ServerRemoveConflictingEntry(args.PrevLogIndex + 1)
		rf.sstate.rw.RLock()
		rf.persist()
		rf.sstate.rw.RUnlock()
		reply.Term = rf.bstate.CurrentTerm
		reply.Success = false
		reply.XHighestIndex = rf.ServerHighestEntryIndex()
		if reply.XHighestIndex >= args.PrevLogIndex {
			reply.XTerm = rf.lstate.Log[rf.ServerIndexTransformer(args.PrevLogIndex)].CommandTerm
			reply.XIndex = rf.ServerFastLogFronttracking(args.PrevLogIndex)
		} else {
			reply.XTerm = 0
			reply.XIndex = 0
		}
		return
	} else if len(args.Entries) > 0 {
		rf.ServerAppendEntries(args.PrevLogIndex+1, args.Entries)
	} else if len(args.Entries) == 0 {
		//nothing to do
	}
	reply.XTerm = 0
	reply.XIndex = 0
	reply.XHighestIndex = rf.ServerHighestEntryIndex()
	reply.Term = rf.bstate.CurrentTerm
	reply.Success = true
	rf.ServerUpdateCommitIndex(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
}
func (rf *Raft) FollowerRecvRV(args *RequestVoteArgs, reply *RequestVoteReply) {
	//pre:nothing
	//cur:bstate.L,lstate.R
	rf.bstate.rw.Lock()
	rf.lstate.rw.RLock()
	rf.lstate.rw.RUnlock()
	defer rf.bstate.rw.Unlock()
	if args.Term < rf.bstate.CurrentTerm {
		reply.Term = rf.bstate.CurrentTerm
		reply.VoteGranted = false
		return
	}
	rf.lstate.rw.RLock()
	logIsCompatiable := rf.ServerCheckSenderLog(args.LastLogIndex, args.LastLogTerm)
	rf.lstate.rw.RUnlock()
	if args.Term > rf.bstate.CurrentTerm {
		rf.vc.rw.Lock()
		defer rf.vc.rw.Unlock()
		rf.ServerRecvLargerTerm(args.Term)
		rf.lstate.rw.RLock()
		rf.sstate.rw.RLock()
		rf.persist()
		rf.sstate.rw.RUnlock()
		rf.lstate.rw.RUnlock()
	}

	if (rf.bstate.VotedFor == null || rf.bstate.VotedFor == args.CandidateId) && logIsCompatiable {
		rf.ServerGrantVote(args.CandidateId)
		reply.Term = rf.bstate.CurrentTerm
		reply.VoteGranted = true
	} else {
		reply.Term = rf.bstate.CurrentTerm
		reply.VoteGranted = false
	}
}
func (rf *Raft) FollowerToCandidate() {
	//pre:bstate.L
	//cur:
	rf.bstate.State = Candidate
	rf.ServerToCandidate()
}
func (rf *Raft) FollowerTicker() {
	//pre:bstate.L
	//cur:
	rf.FollowerToCandidate()
}
func (rf *Raft) ServerRecvLargerTerm(newTerm int) {
	//pre:bstate.L，vc.L
	//cur:
	if newTerm <= rf.bstate.CurrentTerm {
		return
	}
	rf.bstate.CurrentTerm = newTerm
	rf.ServerToFollower()
}
func (rf *Raft) ServerToFollower() {
	//pre:bstate.L,vc.L
	//cur:
	rf.bstate.State = Follower
	rf.bstate.VotedFor = null
	rf.vc.VoteCount = 0
	rf.vc.UnVoteCount = 0
	//if after resetting timer,the ticker enter into any ticker function,what will happen
}
func (rf *Raft) ServerGrantVote(vote int) {
	//pre:bstate.L
	//cur:
	rf.bstate.VotedFor = vote
	rf.ServerResetTimer()
}
func (rf *Raft) ServerHighestEntryIndex() int {
	//pre:lstate.R
	//cur:
	// the highest  Index of Log Entry replicated on this server
	return rf.lstate.HighestIndex
}
func (rf *Raft) ServerToCandidate() {
	//pre:bstate.L
	//cur:vc.L,lstate.R,bstate.R
	rf.ServerGrantVote(rf.me)
	rf.bstate.CurrentTerm += 1
	rf.mu.Lock()
	rf.electionTimeout = rf.ServerGetRandomTimeout(LWRBound, UPRBound)
	rf.mu.Unlock()
	rf.ServerResetTimer()
	rf.vc.rw.Lock()
	rf.vc.VoteCount = 1
	rf.vc.UnVoteCount = 0
	rf.vc.rw.Unlock()

	rf.lstate.rw.RLock()
	rf.sstate.rw.RLock()
	index := len(rf.lstate.Log)
	var term int
	if index != 0 {
		term = rf.lstate.Log[index-1].CommandTerm
	} else {
		term = rf.sstate.LastIncludedTerm
	}
	rf.sstate.rw.RUnlock()
	rf.lstate.rw.RUnlock()
	var RVArgs = RequestVoteArgs{
		Term:         rf.bstate.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lstate.BaseIndex + index - 1,
		LastLogTerm:  term,
	}
	rf.bstate.rw.Unlock()
	rf.bstate.wg.Add(len(rf.peers))
	rf.bstate.rw.RLock()
	for index := range rf.peers {
		go rf.CandidateSendRV(index, &RVArgs)
	}
	rf.bstate.rw.RUnlock()
	rf.bstate.wg.Wait()
}
func (rf *Raft) ServerGetRandomTimeout(LWRBound timeout, UPRBound timeout) int {
	// Seeding with the same value results in the same random sequence each run.
	// For different numbers, seed with a different value, such as
	// time.Now().UnixNano(), which yields a constantly-changing number.
	rand.Seed(time.Now().UnixNano() + int64(rf.me))
	return int(LWRBound) + rand.Intn(int(UPRBound-LWRBound))
}
func (rf *Raft) ServerCheckSenderLog(lastLogIndex int, lastLogTerm int) bool {
	//pre:lstate.R,bstate.L
	//cur
	//If the logs have last entries with different terms, then
	//the log with the later term is more up-to-date. If the logs
	//end with the same term, then whichever log is longer is
	//more up-to-date.
	highestIndex := rf.ServerIndexTransformer(rf.ServerHighestEntryIndex())
	if highestIndex < 0 && rf.ServerIndexTransformer(lastLogIndex) >= 0 {
		return true
	}
	var highestTerm int
	if highestIndex == -1 {
		rf.sstate.rw.RLock()
		highestTerm = rf.sstate.LastIncludedTerm
		rf.sstate.rw.RUnlock()
	} else if highestIndex > -1 {
		highestTerm = rf.lstate.Log[highestIndex].CommandTerm
	} else {
		panic("less than LastIncludeIndex")
	}
	if lastLogTerm < highestTerm || (lastLogTerm == highestTerm && rf.ServerIndexTransformer(lastLogIndex) < highestIndex) {
		return false
	}
	return true
}
func (rf *Raft) ServerHasLogEntry(prevLogIndex int, prevLogTerm int) bool {
	//pre:bstate.L,lstate.L,mIndex.L
	//cur:
	if rf.ServerHighestEntryIndex() < prevLogIndex {
		return false
	}
	i := rf.ServerIndexTransformer(prevLogIndex)
	if prevLogIndex == 0 {
		return true
	}

	rf.sstate.rw.RLock()
	defer rf.sstate.rw.RUnlock()
	if prevLogIndex == rf.sstate.LastIncludedIndex {
		return rf.sstate.LastIncludedTerm == prevLogTerm
	} else if prevLogIndex < rf.sstate.LastIncludedIndex {
		return true
	}

	return rf.lstate.Log[i].CommandTerm == prevLogTerm
}
func (rf *Raft) ServerRemoveConflictingEntry(conflictingLogIndex int) {
	//pre:bstate.L,lstate.L,mIndex.L
	//cur:
	//removing all the entries after it(including itself)
	highestIndex := rf.ServerHighestEntryIndex()
	if highestIndex < conflictingLogIndex {
		return
	}
	rf.ServerRemoveEntry(conflictingLogIndex, highestIndex)

}
func (rf *Raft) ServerAppendEntries(AppendLogIndex int, Entries []Entry) {
	//pre:bstate.L,lstate.L,mIndex.L
	//cur:
	//Appending entries from a given index
	highest := rf.ServerHighestEntryIndex()
	for ei, i, end := 0, AppendLogIndex, AppendLogIndex+len(Entries); i < end; i++ {
		if i < rf.lstate.BaseIndex {
			ei++
			continue
		}
		if i > highest {
			rf.lstate.Log = append(rf.lstate.Log, Entries[ei:]...)
			rf.lstate.HighestIndex = rf.lstate.BaseIndex + len(rf.lstate.Log) - 1
			rf.sstate.rw.RLock()
			rf.persist()
			rf.sstate.rw.RUnlock()
			break
		}
		if Entries[ei].CommandTerm != rf.lstate.Log[rf.ServerIndexTransformer(i)].CommandTerm {
			rf.ServerRemoveConflictingEntry(i)
			rf.lstate.Log = append(rf.lstate.Log, Entries[ei:]...)
			rf.lstate.HighestIndex = rf.lstate.BaseIndex + len(rf.lstate.Log) - 1
			rf.sstate.rw.RLock()
			rf.persist()
			rf.sstate.rw.RUnlock()
			break
		}
		ei++
	}

}
func (rf *Raft) ServerUpdateCommitIndex(LeaderCommit int, NewLogEntryIndex int) {
	//pre:bstate.L,lstate.L,mIndex.L
	//cur:
	//commitIndex = min(leaderCommit, index of last new entry)
	i := LeaderCommit
	if i > NewLogEntryIndex {
		i = NewLogEntryIndex
	}
	if rf.mIndex.CommitIndex < i {
		rf.mIndex.CommitIndex = i
		//persist()
		rf.sstate.rw.RLock()
		rf.persist()
		rf.sstate.rw.RUnlock()
	}
}
func (rf *Raft) ServerResetTimer() {
	rf.mu.Lock()
	t := rf.electionTimeout
	rf.tp.timer.Reset(time.Duration(t) * time.Millisecond)
	rf.mu.Unlock()

}
func (rf *Raft) ServerWaitToWakeup() {
	<-rf.tp.timer.C
}
func (rf *Raft) ServerPrevLogIndex(server int) int {
	//pre:PIndex.R
	//cur:nothing
	return rf.pIndex.nextIndex[server] - 1
}
func (rf *Raft) ServerPrevLogTerm(server int) int {
	//pre:lstate.R，pIndex.R
	//cur:nothing
	LogIndex := rf.pIndex.nextIndex[server] - 1
	if LogIndex == 0 {
		return 0
	}
	rf.sstate.rw.rw.RLock()
	defer rf.sstate.rw.rw.RUnlock()
	if LogIndex == rf.sstate.LastIncludedIndex {
		return rf.sstate.LastIncludedTerm
	}
	return rf.lstate.Log[rf.ServerIndexTransformer(LogIndex)].CommandTerm
}
func (rf *Raft) ServerFastLogBacktracking(curIndex int) int {
	//pre:lstate.L
	//cur:
	ci := rf.ServerIndexTransformer(curIndex)
	hi := rf.ServerIndexTransformer(rf.ServerHighestEntryIndex())
	if ci < 0 {
		panic("ServerFastLogBacktracking: ci<0")
	}
	Term := rf.lstate.Log[ci].CommandTerm
	ret := ci
	for i := ci; i <= hi; i++ {
		if i == hi || rf.lstate.Log[i+1].CommandTerm != Term {
			ret = i
			break
		}
	}
	return ret + rf.lstate.BaseIndex
}
func (rf *Raft) ServerFastLogFronttracking(curIndex int) int {
	//pre:lstate.L
	//cur:
	ci := rf.ServerIndexTransformer(curIndex)
	if ci < 0 {
		panic("ServerFastLogBacktracking: ci<0")
	}
	Term := rf.lstate.Log[ci].CommandTerm
	ret := ci
	for i := ci; i >= 0; i-- {
		if i == 0 || rf.lstate.Log[i-1].CommandTerm != Term {
			ret = i
			break
		}
	}
	return ret + rf.lstate.BaseIndex
}
func (rf *Raft) ServerContainsTerm(Term int) (bool, int) {
	//pre:lstate.L
	//cur:
	i1 := 0
	i2 := len(rf.lstate.Log) - 1
	k := -1
	for i1 < i2 {
		k = (i1 + i2) / 2
		if rf.lstate.Log[k].CommandTerm < Term {
			i1 = k + 1
		} else if rf.lstate.Log[k].CommandTerm > Term {
			i2 = k - 1
		} else {
			break
		}
	}
	if i1 >= i2 {
		return false, -1
	}
	return true, rf.ServerFastLogBacktracking(rf.lstate.BaseIndex + k)
}
func (rf *Raft) ServerIndexTransformer(index int) int {
	//pre:lstate.L
	//cur:
	return index - rf.lstate.BaseIndex
}
func (rf *Raft) ServerReadSlice(start int, end int) []Entry {
	//pre:lstate.L
	//cur:
	if start > end {
		return make([]Entry, 0)
	}
	return rf.lstate.Log[start : end+1]
}
func (rf *Raft) ServerTrimEntry(start int, end int) {
	//[start,end]
	//pre:bstate.L,lstate.L,mIndex.L
	//cur:
	if start > end {
		panic("ServerTrimEntry:start>end!")
		return
	}
	if start > rf.lstate.HighestIndex {
		panic("ServerTrimEntry:start>highestIndex")
	}
	if start < rf.lstate.BaseIndex {
		panic("ServerTrimEntry:start<baseIndex")
	}
	if end < rf.lstate.BaseIndex {
		panic("ServerTrimEntry:end<baseIndex")
	}
	if end > rf.lstate.HighestIndex {
		end = rf.lstate.HighestIndex
	}
	i1 := rf.ServerIndexTransformer(start)
	i2 := rf.ServerIndexTransformer(end)
	if i1 == 0 {
		rf.lstate.BaseIndex = end + 1
	}
	rf.lstate.Log = append(rf.lstate.Log[:i1], rf.lstate.Log[i2+1:]...)
	if i1 != 0 {
		rf.lstate.HighestIndex = rf.lstate.BaseIndex + len(rf.lstate.Log) - 1
	} else {
		rf.lstate.HighestIndex = rf.lstate.BaseIndex + len(rf.lstate.Log) - 1
	}
}
func (rf *Raft) ServerRemoveEntry(start int, end int) {
	//[start,end]
	//pre:bstate.L,lstate.L,mIndex.L
	//cur:
	if start > end {
		panic("ServerRemoveEntry:start>end!")
		return
	}
	if start > rf.lstate.HighestIndex {
		panic("ServerRemoveEntry:start>highestIndex")
	}
	if start < rf.lstate.BaseIndex {
		panic("ServerRemoveEntry:start<baseIndex")
	}
	if end < rf.lstate.BaseIndex {
		panic("ServerRemoveEntry:end<baseIndex")
	}
	if end > rf.lstate.HighestIndex {
		end = rf.lstate.HighestIndex
	}
	i1 := rf.ServerIndexTransformer(start)
	i2 := rf.ServerIndexTransformer(end)

	rf.lstate.Log = append(rf.lstate.Log[:i1], rf.lstate.Log[i2+1:]...)
	if i1 != 0 {
		rf.lstate.HighestIndex = rf.lstate.BaseIndex + len(rf.lstate.Log) - 1
	} else {
		rf.lstate.HighestIndex = rf.lstate.BaseIndex + len(rf.lstate.Log) - 1
	}
}
