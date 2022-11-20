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
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
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

// ElectionTimeoutBase millisecond is the minimum election timeout.
// ElectionTimeoutRange millisecond is used by the peer to randomized at the
// start of an election.
// ElectionTimeout is [ElectionTimeoutBase,ElectionTimeoutBase+ElectionTimeoutRange].
// HeartbeatTimeout is the timeout for the heartbeat of the Leader sent to the follower
// the tester limits you to 10 heartbeats per second
const (
	ElectionTimeoutBase  int = 300
	ElectionTimeoutRange int = 300
	HeartbeatTimeout     int = 50
)

// DecrementSpeed is used by Leader's matchIndex[server] to decrease value
// when the prevLogIndex's log has conflict with the follower.
// TODO:
// Forward searching to find the previous log
// because the the number of log is not specific.
const DecrementSpeed int = 50

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

	// true indicates the peer has received heartbeat in the past period of time.
	heartBeat bool

	X        int
	snapshot []byte

	// used by apply goroutine to apply entry
	applyCond *sync.Cond

	// globalHeartBeat is for measuring time interval only used in tests.
	// such as the goroutine running's time interval or receiving heartbeat's time interval
	globalHeartBeat time.Time
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		fmt.Println(err)
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		fmt.Println(err)
	}

	err = e.Encode(rf.log)
	if err != nil {
		fmt.Println(err)
	}

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voteFor int
	var log []Entry = make([]Entry, 0)

	if d.Decode(&term) != nil || d.Decode(&voteFor) != nil || d.Decode(&log) != nil {
		fmt.Println("readPersist failed")
	} else {
		rf.currentTerm = term
		rf.votedFor = voteFor
		rf.log = log
		rf.X = rf.log[0].Index
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	ReplyTerm int
}

// send the entire snapshot in a single InstallSnapshot.
// Don't implement Figure 13's offset mechanism
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.ReplyTerm = rf.currentTerm
	if rf.currentTerm > args.Term {
		rf.mu.Unlock()
		return
	}

	defer PDebug(file_line(), reply)
	defer PDebug(file_line(), args)
	defer PDebug(file_line(), rf)

	rf.snapshot = args.Data

	defer func() {
		rf.mu.Unlock()
		// save snapshot file,discard any existing or partial snapshot
		// with a smaller index
		// After install snapshot,the lock is release,the rf.X mat changed.
		rf.Snapshot(args.LastIncludedIndex, rf.snapshot)
		// reset state machine using snapshot contents
		go func() {
			rf.mu.Lock()
			if len(rf.snapshot) == 0 {
				rf.mu.Unlock()
				return
			}
			applyMsg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  args.LastIncludedTerm,
				SnapshotIndex: args.LastIncludedIndex,
			}
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
		}()
	}()

	// if the snapshot follow behind the peer ????
	size := len(rf.log)
	if size-1 >= args.LastIncludedIndex-rf.X && args.LastIncludedIndex-rf.X >= 0 {
		if rf.log[args.LastIncludedIndex-rf.X].Term == args.LastIncludedTerm {
			rf.log = rf.log[args.LastIncludedIndex-rf.X:]
			rf.X = args.LastIncludedIndex
			return
		}
	}

	//discard entire log
	rf.log = make([]Entry, 1)
	rf.log[0].Index = args.LastIncludedIndex
	rf.log[0].Term = args.LastIncludedTerm

	rf.X = args.LastIncludedIndex

	// install only happens in follower
	// so matchIndex don't need to update
}

// have the leader send an InstallSnapshot RPC if it doesn't have the log entries
// required to bring a follower up to date.
// send the entire snapshot in a single InstallSnapshot.
// Don't implement Figure 13's offset mechanism.
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// TODO:
	// Because the applyMsg sent to the channel must
	// be in order.So the applyCh <- msg must be executed with lock
	// not true let me think it
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer PDebug(file_line(), rf)
	defer PDebug(file_line()+" index:", index)
	defer PDebug(file_line(), snapshot)

	rf.log = rf.log[index-rf.X:]
	rf.X = index
	rf.snapshot = snapshot
	// snapshot replace log
	rf.lastApplied = index

	if rf.state == StateLeader {
		for idx := range rf.matchIndex {
			// matchIndex don't need to update and don't should be used to send to the peer
			rf.nextIndex[idx] = maxInt(rf.nextIndex[idx], rf.X+1)
		}
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		fmt.Println(err)
	}

	err = e.Encode(rf.votedFor)
	if err != nil {
		fmt.Println(err)
	}

	err = e.Encode(rf.log)
	if err != nil {
		fmt.Println(err)
	}

	data := w.Bytes()
	if len(snapshot) == 0 {
		rf.persister.SaveStateAndSnapshot(data, nil)
	} else {
		rf.persister.SaveStateAndSnapshot(data, snapshot)
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer PDebug(file_line(), reply)
	defer PDebug(file_line(), args)
	defer PDebug(file_line(), rf)

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < args.Term {
		rf.becomeFollower(args.Term)
	}

	reply.Term = rf.currentTerm

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// verify the candidate's log is at least up-to-date as receiver's log.
		rfLastTerm := rf.log[len(rf.log)-1].Term

		if rfLastTerm > args.LastLogTerm {
			return
		}

		if rfLastTerm == args.LastLogTerm && args.LastLogIndex-rf.X < len(rf.log)-1 {
			return
		}

		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		// granting vote to candidate can be view as receiving heartbeat.
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

	defer PDebug(file_line(), rf)
	defer PDebug(file_line(), args)
	defer PDebug(file_line(), reply)

	defer func() {
		// if the leaderCommit > commitIndex,set
		// commitIndex = min(leaderCommit,index of the last new entry).
		if args.LeaderCommit > rf.commitIndex {
			// reply's Success indicates the peer's prevLogIndex and prevLogTerm matching the leader.
			// so the last entry must be up-to-date with the leader.
			// the previous conflicting entries has been deleted.
			// we can safely commit the new entries.
			if !reply.Success {
				return
			}
			// because of the applyCh can be blocked so need to start a new goroutine
			// and the entry need to be sent in order.
			commitIndex := minInt(args.PrevLogIndex, args.LeaderCommit)
			// first update the rf's commitIndex
			// to avoid the other RPC to commit the command
			idx := rf.commitIndex + 1
			if idx > commitIndex {
				return
			}
			rf.commitIndex = commitIndex
			rf.applyCond.Signal()
		}
	}()

	reply.Success = true

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	// the not-null entries also can make effect as the heartbeat.
	rf.heartBeat = true

	// empty for heartbeat
	if len(args.Entries) == 0 {
	}

	// received the AppendEntries RPC from the new leader
	// convert to the follower.
	if rf.state == StateCandidate {
		rf.state = StateFollower
	}

	size := len(rf.log)

	// lack of new entries
	if args.PrevLogIndex-rf.X > size-1 {
		reply.Success = false
		return
	}

	if args.PrevLogIndex-rf.X < 0 {
		reply.Success = false
		return
	}

	// PrevLogIndex's log conflicts with a new one
	// delete the existing entry and all that follow it.
	if rf.log[args.PrevLogIndex-rf.X].Term != args.PrevLogTerm {
		rf.log = rf.log[:args.PrevLogIndex-rf.X]
		rf.persist()
		reply.Success = false
		return
	}

	if reply.Success {
		// Append new entries to the receiver's log.
		for _, entry := range args.Entries {
			if entry.Index-rf.X <= len(rf.log)-1 {
				if rf.log[entry.Index-rf.X].Term != entry.Term {
					rf.log = rf.log[:entry.Index-rf.X]
					rf.log = append(rf.log, entry)
				}
				// This can avoid the one AppendEntries RPC come later but the contained entries are the same.
				// such as:
				// rf.log  [1,2,data] [2,2,data] [3,2,data]
				// entries [1,2,data] [2,2,data]
				// entries is shorter than rf.log but the same as it.
			} else {
				rf.log = append(rf.log, entry)
			}
		}
		rf.persist()
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
	index = logSize + rf.X

	entry := Entry{}
	entry.Date = command
	entry.Term = term
	entry.Index = index

	rf.log = append(rf.log, entry)
	rf.persist()
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
		electionTimeout := (rand.Int() % ElectionTimeoutRange) + ElectionTimeoutBase
		time.Sleep(time.Millisecond * time.Duration(electionTimeout))
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if !rf.heartBeat {
			// start election
			rf.state = StateCandidate
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.persist()
		} else {
			rf.heartBeat = false
			rf.mu.Unlock()
			continue
		}
		args := &RequestVoteArgs{}
		args.Term = rf.currentTerm
		entry := rf.log[len(rf.log)-1]
		rf.mu.Unlock()

		// to verify the candidate's log is at least as up-to-date as with receiver
		// definition is in the end of the Section 5.4.1
		args.LastLogIndex = entry.Index
		args.LastLogTerm = entry.Term
		args.CandidateId = rf.me
		count := int32(1)

		//PDebug(file_line(), rf)
		//PDebug(file_line(), args)

		for id := range rf.peers {
			if id == rf.me {
				continue
			}
			server := id
			go func() {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, args, &reply)
				if !ok {
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
				// if the peer isn't StateCandidate and the term is changed
				// we need to return immediately because the RequestVote is useless
				if rf.state != StateCandidate || rf.currentTerm != args.Term {
					rf.mu.Unlock()
					return
				}

				// hold lock to ensure consistency
				atomic.AddInt32(&count, 1)
				if int(count) > len(rf.peers)/2 {
					rf.state = StateLeader
					for sid := range rf.matchIndex {
						rf.matchIndex[sid] = 0
						// int the following,we need to avoid nextIndex < rf.X.
						rf.nextIndex[sid] = len(rf.log) + rf.X
					}
					// immediately send heartbeats
					go rf.sendHeartbeat(rf.currentTerm)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}()
		}
	}
}

// term is the peer's turned into leader upon election used by
// terminating the useless goroutine
func (rf *Raft) sendHeartbeat(term int) {
	args := AppendEntriesArgs{}

	args.Term = term
	args.LeadId = rf.me

	for rf.killed() == false {
		rf.applyCond.Signal()
		rf.mu.Lock()
		// terminate the useless heartbeat goroutine
		// if the peer's state has changed
		if rf.state != StateLeader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		for id := range rf.peers {
			go func(serverId int) {
				// even if here the peer is not the leader,
				// sending one more heartbeat doesn't affect the cluster's state
				// because of the serverArgs term is not changed.
				// so we release the lock.
				if serverId == rf.me {
					rf.mu.Lock()
					rf.heartBeat = true
					rf.mu.Unlock()
					return
				}

				rf.mu.Lock()
				serverArgs := args
				serverArgs.LeaderCommit = rf.commitIndex
				serverArgs.PrevLogIndex = rf.nextIndex[serverId] - 1
				serverArgs.PrevLogTerm = rf.log[serverArgs.PrevLogIndex-rf.X].Term
				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(serverId, &serverArgs, &reply)
				// TODO:
				// After the sendAppendEntries return,the condition may have changed.
				// we need to pay attention the idempotence of the RPC call.
				// TODO:
				// if the ok is false indicates the receiver is not connected,
				// so we still sleep for HeartbeatTimeout interval?
				// can we sleep for a short time?
				if ok {
					if !reply.Success {
						if reply.Term > serverArgs.Term {
							rf.mu.Lock()
							rf.becomeFollower(reply.Term)
							rf.mu.Unlock()
							return
						} else {
							// if the prevLogIndex entry is not match,the leader needs to decrease the matchIndex.
							// Why this happened in the heartbeat process?
							// Because if the leader doesn't commit an entry next,
							// the log can't be sent to the follower.
							// So we firstly to communicate the matchIndex state with the receiver.
							// And when the matchIndex matches,we can send entries to the receiver.

							// We had better make this call idempotent.
							// This process is idempotent,the repeated snapshot rpc call will not affect the result.
							rf.mu.Lock()
							// send snapshot
							if serverArgs.PrevLogIndex-DecrementSpeed < rf.X {
								snapshotArgs := InstallSnapshotArgs{}
								snapshotArgs.LastIncludedIndex = rf.log[0].Index
								snapshotArgs.LastIncludedTerm = rf.log[0].Term
								snapshotArgs.Data = rf.snapshot
								snapshotArgs.Done = true
								snapshotArgs.Offset = 0
								snapshotArgs.Term = rf.currentTerm
								snapshotArgs.LeaderId = rf.me
								rf.mu.Unlock()
								snapshotReply := InstallSnapshotReply{}
								ok := rf.sendInstallSnapshot(serverId, &snapshotArgs, &snapshotReply)
								// Pay attention to the inconsistency.
								if ok {
									rf.mu.Lock()
									if snapshotReply.ReplyTerm > rf.currentTerm {
										rf.becomeFollower(snapshotReply.ReplyTerm)
										rf.mu.Unlock()
										return
									}
									// we update matchIndex assuming that matchIndex have not been changed.
									if rf.matchIndex[serverId] == 0 {
										rf.matchIndex[serverId] = snapshotArgs.LastIncludedIndex
										rf.nextIndex[serverId] = maxInt(snapshotArgs.LastIncludedIndex+1, rf.X+1)
									}
									rf.mu.Unlock()
								} else {
									return
								}

								//term := rf.currentTerm
								//rf.mu.Unlock()
								//ok = rf.tryToSendSnapshot(term, serverId)
								//if !ok {
								//	return
								//}
								// after install snapshot to send entries
							} else {
								// Pay attention to the inconsistency.
								// We update matchIndex assuming that matchIndex have not been changed.
								// nextIndex should be updated to serverArgs.PrevLogIndex-DecrementSpeed+1
								// but rf.X+1 is the minimum value for nextIndex.
								if rf.matchIndex[serverId] == 0 {
									rf.nextIndex[serverId] = maxInt(serverArgs.PrevLogIndex-DecrementSpeed+1, rf.X+1)
								}
								rf.mu.Unlock()
								return
							}
							// We sleep HeartbeatTimeout because there is no need to communicate as soon as possible
							// and also in order to avoid the conflict with the AppendEntry RPC.
							// time.Sleep(time.Duration(HeartbeatTimeout) * time.Millisecond)
							//continue
						}
					}

					// if the previous steps make matchIndex != 0,so we can send more entries to the follower.

					// reply.Success is true means the nextIndex match with the receiver.
					// So we can send all following entries to the receiver.
					// If failed,don't need to retry,because this process can happen in the next heartbeat.
					rf.mu.Lock()
					// Pay attention to the inconsistency.
					// We send more entries assuming that matchIndex have not been changed.
					if rf.matchIndex[serverId] > serverArgs.PrevLogIndex {
						rf.mu.Unlock()
						return
					}
					rf.matchIndex[serverId] = serverArgs.PrevLogIndex
					// The rf.X may change during the period.
					rf.nextIndex[serverId] = maxInt(serverArgs.PrevLogIndex+1, rf.X+1)
					nextIdx := rf.nextIndex[serverId]

					if nextIdx-rf.X <= len(rf.log)-1 && nextIdx > rf.X {
						serverArgs.PrevLogIndex = rf.nextIndex[serverId] - 1
						serverArgs.PrevLogTerm = rf.log[serverArgs.PrevLogIndex-rf.X].Term
						entryArgs := serverArgs
						for nextIdx-rf.X < len(rf.log) {
							entryArgs.Entries = append(entryArgs.Entries, rf.log[nextIdx-rf.X])
							nextIdx++
						}
						rf.mu.Unlock()
						reply = AppendEntriesReply{}
						ok := rf.sendAppendEntries(serverId, &entryArgs, &reply)

						if ok {
							rf.mu.Lock()
							if reply.Term > rf.currentTerm {
								rf.becomeFollower(reply.Term)
								rf.mu.Unlock()
								return
							}
							rf.mu.Unlock()
							if reply.Success {
								rf.mu.Lock()
								// Pay attention to the inconsistency.
								// We update matchIndex assuming that matchIndex have not been changed.
								// If the matchIndex changed,we update may back the state.
								if rf.matchIndex[serverId] >= nextIdx-1 {
									rf.mu.Unlock()
									return
								}
								rf.matchIndex[serverId] = nextIdx - 1
								rf.nextIndex[serverId] = maxInt(nextIdx, rf.X+1)
								rf.mu.Unlock()
							}
						}
					} else {
						rf.mu.Unlock()
					}
				}
			}(id)
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
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) tryToCommitEntry(term int, entry *Entry) {
	count := int32(1)

	for id := range rf.peers {
		if id == rf.me {
			continue
		}
		go func(serverId int) {
			for {
				args := AppendEntriesArgs{}
				args.Term = term

				rf.mu.Lock()
				// verify peer's state to terminate the useless goroutine
				if rf.state != StateLeader {
					rf.mu.Unlock()
					return
				}

				// the leader already has a new Entry to be committed
				// so the old entry's goroutine can be terminated.
				// because the new entry's goroutine can be responsible for sending the all entries.
				if entry.Index-rf.X < len(rf.log)-1 {
					rf.mu.Unlock()
					return
				}

				if entry.Index <= rf.matchIndex[serverId] {
					rf.mu.Unlock()
					return
				}

				args.LeaderCommit = rf.commitIndex
				args.LeadId = rf.me

				args.PrevLogIndex = rf.nextIndex[serverId] - 1
				args.PrevLogTerm = rf.log[args.PrevLogIndex-rf.X].Term

				idx := rf.nextIndex[serverId]

				for idx-rf.X < len(rf.log) {
					args.Entries = append(args.Entries, rf.log[idx-rf.X])
					idx++
				}
				rf.mu.Unlock()

				reply := AppendEntriesReply{}

				// TODO:
				ok := rf.sendAppendEntries(serverId, &args, &reply)
				// if the receiver in not connected,we need to retry.
				if !ok {
					//time.Sleep(time.Millisecond * 10)
					continue
				}

				if reply.Term > args.Term {
					rf.mu.Lock()
					rf.becomeFollower(reply.Term)
					rf.mu.Unlock()
					return
				}

				// same logic in sendHeartbeat
				if !reply.Success {
					rf.mu.Lock()
					// send snapshot
					if args.PrevLogIndex-DecrementSpeed < rf.X {
						snapshotArgs := InstallSnapshotArgs{}
						snapshotArgs.LastIncludedIndex = rf.log[0].Index
						snapshotArgs.LastIncludedTerm = rf.log[0].Term
						snapshotArgs.Data = rf.snapshot
						snapshotArgs.Done = true
						snapshotArgs.Offset = 0
						snapshotArgs.Term = rf.currentTerm
						snapshotArgs.LeaderId = rf.me
						rf.mu.Unlock()
						reply := InstallSnapshotReply{}
						ok := rf.sendInstallSnapshot(serverId, &snapshotArgs, &reply)
						if ok {
							rf.mu.Lock()
							if reply.ReplyTerm > snapshotArgs.Term {
								rf.becomeFollower(reply.ReplyTerm)
								rf.mu.Unlock()
								return
							}
							if rf.matchIndex[serverId] < snapshotArgs.LastIncludedIndex {
								rf.matchIndex[serverId] = snapshotArgs.LastIncludedIndex
								rf.nextIndex[serverId] = maxInt(snapshotArgs.LastIncludedIndex+1, rf.X+1)
							}
							rf.mu.Unlock()
						}
					} else {
						rf.nextIndex[serverId] = maxInt(args.PrevLogIndex-DecrementSpeed+1, rf.X+1)
						rf.mu.Unlock()
					}
					// because the matchIndex entry conflicts,so we need to retry to send.
					// here we sleep for a short interval in order to reach the consistence quickly.
					// if we not sleep,goroutine's starvation may occur because of the lock held by other goroutine.
					continue
				}

				rf.mu.Lock()
				if idx-1 > rf.matchIndex[serverId] {
					rf.matchIndex[serverId] = idx - 1
					rf.nextIndex[serverId] = maxInt(idx, rf.X)
				}
				rf.mu.Unlock()

				// don't need to verify the peer's state even if the peer isn't leader now.
				// because the log has already spread the majority of the peers.
				// the later leader must have this entry too.!!!
				// so we can safely commit the entry.

				// If the leader election happens after the log spread,the new leader must have this new log.
				// If the leader election happens before the log spread,the count > len(peers)/2 must be false.
				// Detailed explain is in Section 5.4.3 and Figure 9.
				atomic.AddInt32(&count, 1)
				c := atomic.LoadInt32(&count)
				if int(c) > len(rf.peers)/2 {
					commitIndex := args.PrevLogIndex + len(args.Entries)
					rf.mu.Lock()
					if rf.commitIndex < commitIndex {
						// first update the commitIndex
						// in order to avoid sending command again
						rf.commitIndex = commitIndex
					} else {
						rf.mu.Unlock()
						return
					}

					rf.applyCond.Signal()
					rf.mu.Unlock()
					// avoid the committing process happens again.
					atomic.AddInt32(&count, -c)
				}
				return
			}
		}(id)
	}
}

// if the goroutine doesn't wait on the applyCond,
// the other goroutine wake up is useless.
func (rf *Raft) applyEntries() {
	//rf.mu.Lock()
	for {
		rf.applyCond.L.Lock()
		for rf.lastApplied == rf.commitIndex {
			rf.applyCond.Wait()
		}
		rf.applyCond.L.Unlock()

		rf.mu.Lock()
		commitIndex := rf.commitIndex
		idx := rf.lastApplied + 1
		PDebug(file_line(), rf)
		PDebug(file_line(), commitIndex)
		// idx < rf.X avoid the rf.X changed
		for idx <= rf.commitIndex && idx > rf.X {
			msg := ApplyMsg{}
			msg.CommandValid = true
			msg.Command = rf.log[idx-rf.X].Date
			msg.CommandIndex = idx
			rf.mu.Unlock()
			// if the snapshot install happens before the msg send to the channel.
			rf.applyCh <- msg
			rf.mu.Lock()
			idx++
		}
		rf.lastApplied = maxInt(rf.commitIndex, rf.lastApplied)
		rf.mu.Unlock()
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
	rf.votedFor = -1

	// if is restarted,does it will apply again and error occur?
	// Yes.But apply again to restore the state machine is the meaning og log.
	rf.commitIndex = 0
	rf.lastApplied = 0
	//m := sync.Mutex{}
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.state = StateFollower
	// the log[0] is a base log for communicating with other peers.
	rf.log = make([]Entry, 1)
	rf.log[0].Index = 0
	rf.log[0].Term = 0

	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.X = rf.log[0].Index

	for idx := range rf.nextIndex {
		rf.matchIndex[idx] = 0
		rf.nextIndex[idx] = rf.X + 1
	}

	rf.globalHeartBeat = time.Now()
	// start ticker goroutine to start elections
	go rf.applyEntries()
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
	return b
}

func PDebug(lineno string, arg interface{}) {
	if IsDebug {
		fmt.Printf("%s  %+v\n", lineno, arg)
	}
}

// print the file name and line number
func file_line() string {
	_, fileName, fileLine, ok := runtime.Caller(1)
	fileName = filepath.Base(fileName)
	var s string
	if ok {
		s = fmt.Sprintf("%s:%d", fileName, fileLine)
	} else {
		s = ""
	}
	return s
}

const IsDebug = false
const IsTimeMeasure = false

// for measuring the time elapsed
func Elapsed(hint string, start *time.Time, rf *Raft) {
	if IsTimeMeasure {
		fmt.Printf("%s leader:%d elapsed:%s term:%d\n", hint, rf.me, time.Since(*start), rf.currentTerm)
		*start = time.Now()
	}
}
