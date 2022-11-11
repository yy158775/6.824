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
	"math/rand"
	"time"
	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// StateType represents the role of a raft peer in a cluster.
type StateType int

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)
const (
	ElectionTimeoutBase  int = 300
	ElectionTimeoutRange int = 400
	HeartbeatTimeout     int = 150
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
	state   StateType
	applyCh chan ApplyMsg

	currentTerm int
	votedFor    int
	log         []Entry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	// heartbeat
	heartBeat bool
}

type Entry struct {
	Term  int
	Index int
	Date  interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	state := rf.state
	term := rf.currentTerm
	rf.mu.Unlock()

	if state == StateLeader {
		return term, true
	}
	return term, false
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
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// heartbeat

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < args.Term {
		rf.becomeFollower(args.Term)
	}

	reply.Term = rf.currentTerm

	if rf.votedFor == 0 || rf.votedFor == args.CandidateId {
		rfLastTerm := rf.log[len(rf.log)-1].Term

		if rfLastTerm > args.LastLogTerm {
			return
		}

		if rfLastTerm == args.LastLogTerm && args.LastLogIndex < len(rf.log)-1 {
			return
		}

		//fmt.Printf("Peer:%d to be leader\n", args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.heartBeat = true
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return
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

type AppendEntriesArgs struct {
	Term         int
	LeadId       int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("Peer:%d len(logs):%d commit:%d Sender:%d,term:%d,sender commit:%d\n", rf.me, len(rf.log), rf.commitIndex, args.LeadId, args.Term, args.LeaderCommit)
	//fmt.Printf("sender logs:%v\n", args.Entries)
	//fmt.Printf("receiver logs:%v\n", rf.log)
	defer func() {
		// every peer send to applyCh
		// leader commit but peer not get the entry
		if args.LeaderCommit > rf.commitIndex && args.PrevLogIndex == len(rf.log)-1 && len(args.Entries) == 0 {
			size := len(rf.log)
			commitIndex := minInt(size-1, args.LeaderCommit)
			idx := rf.commitIndex + 1
			for idx <= commitIndex {
				msg := ApplyMsg{}
				msg.CommandValid = true
				msg.CommandIndex = idx
				msg.Command = rf.log[idx].Date
				rf.applyCh <- msg
				idx++
			}
			rf.commitIndex = commitIndex
		}
	}()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// update rf.Term
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	rf.heartBeat = true

	size := len(rf.log)

	if len(args.Entries) == 0 {
		// follower
		if rf.state == StateCandidate {
			rf.state = StateFollower
		}
		return
	}

	// lack of entry
	if args.PrevLogIndex > size-1 {
		reply.Success = false
		return
	}

	if rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
		// append

		for _, entry := range args.Entries {
			if entry.Index <= len(rf.log)-1 {
				if rf.log[entry.Index].Term != entry.Term {
					//fmt.Printf("Peer:%d len(logs):%d delete entry before %d\n", rf.me, len(rf.log), entry.Index)
					rf.log = rf.log[:entry.Index]
					rf.log = append(rf.log, entry)
				}
			} else {
				rf.log = append(rf.log, entry)
			}
		}
		reply.Success = true
		return
	} else {
		reply.Success = false
		rf.log = rf.log[:args.PrevLogIndex]
	}

	return
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state != StateLeader || rf.killed() {
		isLeader = false
		return index, term, isLeader
	}

	logSize := len(rf.log)
	index = logSize

	entry := Entry{}
	entry.Date = command
	entry.Term = term
	entry.Index = index

	rf.log = append(rf.log, entry)
	//fmt.Printf("Leader:%d Term:%d,new entry:%d\n", rf.me, rf.currentTerm, entry.Index)
	go rf.tryToCommitEntry(term, &entry)

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
		electionTimeout := (rand.Int() % ElectionTimeoutRange) + ElectionTimeoutBase
		time.Sleep(time.Millisecond * time.Duration(electionTimeout))
		//fmt.Printf("peer:%d commitIdx:%d\n", rf.me, rf.commitIndex)
		// start election
		rf.mu.Lock()
		// election
		if !rf.heartBeat {
			rf.state = StateCandidate
			rf.currentTerm++
			rf.votedFor = rf.me
		} else {
			rf.heartBeat = false
			rf.mu.Unlock()
			continue
		}

		args := &RequestVoteArgs{}
		args.Term = rf.currentTerm
		entry := rf.log[len(rf.log)-1]
		rf.mu.Unlock()

		args.LastLogIndex = entry.Index
		args.LastLogTerm = entry.Term
		args.CandidateId = rf.me
		count := int32(1)

		for id := range rf.peers {
			if id == rf.me {
				continue
			}
			server := id
			go func() {
				reply := RequestVoteReply{}
				success := rf.sendRequestVote(server, args, &reply)
				if !success {
					return
				}

				rf.mu.Lock()
				if reply.Term > args.Term {
					rf.becomeFollower(reply.Term)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				if !reply.VoteGranted {
					return
				}

				rf.mu.Lock()
				if rf.state != StateCandidate || rf.currentTerm != args.Term {
					rf.mu.Unlock()
					return
				}

				atomic.AddInt32(&count, 1)
				if int(count) > len(rf.peers)/2 {
					rf.state = StateLeader
					for sid := range rf.matchIndex {
						rf.matchIndex[sid] = rf.commitIndex
						rf.nextIndex[sid] = rf.commitIndex + 1
					}
					//fmt.Printf("Peer:%d become Leader\n", rf.me)
					rf.mu.Unlock()
					go rf.sendHeartbeat()
					return
				}
				rf.mu.Unlock()
			}()
		}
	}
	//fmt.Printf("peer:%d Exit ticker\n", rf.me)
}

func (rf *Raft) sendHeartbeat() {
	args := AppendEntriesArgs{}

	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.LeadId = rf.me
	rf.mu.Unlock()

	for rf.killed() == false {
		rf.mu.Lock()
		// terminate the goroutine
		if rf.state != StateLeader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}
		args.LeaderCommit = rf.commitIndex
		rf.mu.Unlock()

		for id := range rf.peers {
			//fmt.Printf("Send heartbeat Leader:%d To:%d commitIdx:%d\n", rf.me,id,rf.commitIndex)
			if id == rf.me {
				rf.mu.Lock()
				rf.heartBeat = true
				rf.mu.Unlock()
				continue
			}
			server := id
			rf.mu.Lock()
			args.PrevLogIndex = rf.matchIndex[server]
			hargs := args
			rf.mu.Unlock()
			go func() {
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &hargs, &reply)
				if !ok {
					return
				}
				rf.mu.Lock()
				if reply.Term > hargs.Term {
					rf.becomeFollower(reply.Term)
				}
				rf.mu.Unlock()
			}()
		}
		time.Sleep(time.Duration(HeartbeatTimeout) * time.Millisecond)
	}
}

func (rf *Raft) becomeFollower(term int) {
	if term <= rf.currentTerm {
		return
	}
	rf.currentTerm = term
	rf.state = StateFollower
	rf.votedFor = 0
	rf.heartBeat = true
}

func (rf *Raft) tryToCommitEntry(term int, entry *Entry) {
	count := int32(1)

	for id := range rf.peers {
		//fmt.Printf("TrytoCommit Leader:%d\n",rf.me)
		server := id
		if server == rf.me {
			continue
		}
		go func() {
			for {
				args := AppendEntriesArgs{}
				args.Term = term

				rf.mu.Lock()
				// Raft already has new Entry
				// Confirm the tryToCommitEntry have the same entries
				// also confirm the matchIndex is the same

				if entry.Index < len(rf.log)-1 {
					rf.mu.Unlock()
					return
				}

				if entry.Index <= rf.matchIndex[server] {
					rf.mu.Unlock()
					return
				}

				args.LeaderCommit = rf.commitIndex
				args.LeadId = rf.me

				args.PrevLogIndex = rf.matchIndex[server]
				args.PrevLogTerm = rf.log[rf.matchIndex[server]].Term
				idx := rf.nextIndex[server]

				for idx < len(rf.log) {
					args.Entries = append(args.Entries, rf.log[idx])
					idx++
				}
				rf.mu.Unlock()

				reply := AppendEntriesReply{}

				ok := rf.sendAppendEntries(server, &args, &reply)
				if !ok {
					return
				}
				fmt.Printf("Peer:%d sendAppend to %d\n", rf.me, server)
				// because other has bigger than me
				if reply.Term > args.Term {
					rf.mu.Lock()
					rf.becomeFollower(reply.Term)
					fmt.Printf("Peer:%d become follower\n", rf.me)
					rf.mu.Unlock()
					return
				}

				if !reply.Success {
					rf.mu.Lock()
					rf.matchIndex[server] = args.PrevLogIndex - 1
					rf.nextIndex[server] = args.PrevLogIndex
					rf.mu.Unlock()
					continue
				}

				rf.mu.Lock()
				if entry.Index > rf.matchIndex[server] {
					rf.matchIndex[server] = entry.Index
					rf.nextIndex[server] = entry.Index + 1
				}
				rf.mu.Unlock()

				atomic.AddInt32(&count, 1)
				if int(count) > len(rf.peers)/2 {
					commitIndex := args.PrevLogIndex + len(args.Entries)
					rf.mu.Lock()
					idx := rf.commitIndex + 1
					if rf.commitIndex < commitIndex {
						rf.commitIndex = commitIndex
					} else {
						rf.mu.Unlock()
						return
					}

					// Leader commit entries
					for idx <= commitIndex {
						msg := ApplyMsg{}
						msg.CommandValid = true
						msg.Command = rf.log[idx].Date
						msg.CommandIndex = idx
						rf.applyCh <- msg
						idx++
					}
					rf.mu.Unlock()
					atomic.AddInt32(&count, -count)
				}
				return
			}
		}()
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = 0

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.state = StateFollower
	rf.log = make([]Entry, 1)
	rf.log[0].Index = 0
	rf.log[0].Term = 0

	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func minInt(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return a
}
