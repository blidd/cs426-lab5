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
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
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

	applyCh chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int32
	votedFor    int32 // candidateId that received vote in current term
	log         []*LogEntry

	commitIndex int32
	lastApplied int32

	nextIndex  []int32 // serverId : index of next log entry to send to it
	matchIndex []int32 // serverId : index of highest log entry replicated on it

	// my stored state
	state           int32
	appendEntriesCh chan struct{}
	voteGrantedCh   chan struct{}
}

// possible states
const (
	Follower         = iota
	Candidate        = iota
	LeaderTransition = iota
	Leader           = iota
)

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

func (rf *Raft) GetCurrentTerm() int32     { return atomic.LoadInt32(&rf.currentTerm) }
func (rf *Raft) IncrementCurrentTerm()     { atomic.AddInt32(&rf.currentTerm, 1) }
func (rf *Raft) SetCurrentTerm(term int32) { atomic.StoreInt32(&rf.currentTerm, term) }

func (rf *Raft) GetVotedFor() int32          { return atomic.LoadInt32(&rf.votedFor) }
func (rf *Raft) SetVotedFor(candidate int32) { atomic.StoreInt32(&rf.votedFor, candidate) }

func (rf *Raft) GetServerState() int32      { return atomic.LoadInt32(&rf.state) }
func (rf *Raft) SetServerState(state int32) { atomic.StoreInt32(&rf.state, state) }

func (rf *Raft) GetCommitIndex() int    { return int(atomic.LoadInt32(&rf.commitIndex)) }
func (rf *Raft) SetCommitIndex(idx int) { atomic.StoreInt32(&rf.commitIndex, int32(idx)) }

func (rf *Raft) GetLastApplied() int    { return int(atomic.LoadInt32(&rf.lastApplied)) }
func (rf *Raft) IncrementLastApplied()  { atomic.AddInt32(&rf.lastApplied, 1) }
func (rf *Raft) SetLastApplied(idx int) { atomic.StoreInt32(&rf.lastApplied, int32(idx)) }

func (rf *Raft) GetNextIndex(i int) int   { return int(atomic.LoadInt32(&rf.nextIndex[i])) }
func (rf *Raft) SetNextIndex(i, idx int)  { atomic.StoreInt32(&rf.nextIndex[i], int32(idx)) }
func (rf *Raft) AddNextIndex(i, num int)  { atomic.AddInt32(&rf.nextIndex[i], int32(num)) }
func (rf *Raft) DecrementNextIndex(i int) { atomic.AddInt32(&rf.nextIndex[i], -1) }

func (rf *Raft) GetMatchIndex(i int) int  { return int(atomic.LoadInt32(&rf.matchIndex[i])) }
func (rf *Raft) SetMatchIndex(i, idx int) { atomic.StoreInt32(&rf.matchIndex[i], int32(idx)) }

type LogEntry struct {
	Term    int
	Command interface{}
}

// return index of entry and pointer to entry itself
func (rf *Raft) GetLastLogEntry() *LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1]
	} else {
		return &LogEntry{Term: -1, Command: nil}
	}
}

func (rf *Raft) GetLogEntry(i int) (*LogEntry, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if i < 0 || len(rf.log) <= i {
		return nil, false
	} else {
		return rf.log[i], true
	}
}

func (rf *Raft) GetLogSuffix(start int) []*LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log[start:]
}

func (rf *Raft) GetLastLogIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.log) - 1
}

func (rf *Raft) MatchLogEntry(prevLogIndex, prevLogTerm int) (int, bool) {
	logLength := rf.GetLastLogIndex() + 1
	if prevLogIndex >= logLength {
		return logLength, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.log[prevLogIndex].Term == prevLogTerm {
		return -1, true
	}
	// if the terms don't match, find the first index it stores for that term
	term := rf.log[prevLogIndex].Term

	i := prevLogIndex
	for i > 0 && rf.log[i-1].Term == term {
		i--
	}

	return i, false
}

func (rf *Raft) IsMajority(x int) bool {
	return x > len(rf.peers)/2
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return int(rf.GetCurrentTerm()), rf.GetServerState() == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.GetCurrentTerm())
	e.Encode(rf.GetVotedFor())
	e.Encode(rf.log)

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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int32
	var votedFor int32
	var peerLog []*LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&peerLog) != nil {
		log.Fatalf("decoding error")
	} else {
		rf.SetCurrentTerm(currentTerm)
		rf.SetVotedFor(votedFor)
		rf.log = peerLog
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int32
	CandidateId  int32
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32
	VoteGranted bool
	Id          int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.GetCurrentTerm()
	if args.Term < rf.GetCurrentTerm() {
		reply.VoteGranted = false
		return
	} else if args.Term > reply.Term {
		rf.SetCurrentTerm(args.Term)
		rf.SetVotedFor(-1)
		rf.SetServerState(Follower)
		rf.mu.Lock()
		rf.persist()
		rf.mu.Unlock()
	}

	if rf.GetVotedFor() < 0 || rf.GetVotedFor() == args.CandidateId {
		// 5.4.1 election restriction: check if the candidate's log is up-to-date
		if CheckUpToDate(
			args.LastLogTerm,
			args.LastLogIndex,
			rf.GetLastLogEntry().Term,
			rf.GetLastLogIndex(),
		) {
			rf.SetVotedFor(args.CandidateId)
			reply.VoteGranted = true
			rf.voteGrantedCh <- struct{}{}
		} else {
			reply.VoteGranted = false
		}
	} else {
		reply.VoteGranted = false
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

// AppendEntries RPC args
type AppendEntriesArgs struct {
	Term         int32
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

// AppendEntries RPC reply
type AppendEntriesReply struct {
	Term          int32
	Success       bool
	NumAppended   int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.GetCurrentTerm()
	if args.Term < rf.GetCurrentTerm() {
		reply.Success = false
		return
	} else if args.Term > rf.GetCurrentTerm() {
		rf.SetCurrentTerm(args.Term)
		rf.SetVotedFor(-1)
		rf.SetServerState(Follower)
		rf.mu.Lock()
		rf.persist()
		rf.mu.Unlock()
	}
	if idx, ok := rf.MatchLogEntry(args.PrevLogIndex, args.PrevLogTerm); !ok {
		reply.Success = false
		reply.ConflictIndex = idx
		rf.appendEntriesCh <- struct{}{}
		return
	}

	rf.mu.Lock()
	var entryIdx int
	for entryIdx = 0; entryIdx < len(args.Entries); entryIdx++ {
		curr := args.PrevLogIndex + 1 + entryIdx
		// log has no existing entries at idx curr, so just break
		if curr >= len(rf.log) {
			break
		}
		// log has entry at idx curr; check if conflicts with new entry
		if rf.log[curr].Term != args.Entries[entryIdx].Term {
			rf.log = rf.log[:curr]
			break
		}
	}
	for _, entry := range args.Entries[entryIdx:] {
		copied := *entry
		rf.log = append(rf.log, &copied)
	}
	rf.persist()
	rf.mu.Unlock()

	rf.appendEntriesCh <- struct{}{}
	reply.Success = true
	reply.NumAppended = len(args.Entries[entryIdx:])

	if args.LeaderCommit > rf.GetCommitIndex() {
		rf.SetCommitIndex(min(args.LeaderCommit, rf.GetLastLogIndex()))
	}
}

func (rf *Raft) DumpLog() {
	logEntries := make([]interface{}, len(rf.log))
	for i, _ := range rf.log {
		logEntries[i] = i
	}
	log.Printf("(%d) log: %+v", rf.me, logEntries)
}

func min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

func RandomTimeout(floor, ceil int) <-chan time.Time {
	ms := rand.Intn(ceil-floor) + floor
	return time.After(time.Duration(ms) * time.Millisecond)
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
	var term int
	var isLeader bool
	if term, isLeader = rf.GetState(); !isLeader {
		return 0, term, false
	}

	// Your code here (2B).
	entry := &LogEntry{
		Term:    term,
		Command: command,
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log = append(rf.log, entry)
	rf.SetMatchIndex(rf.me, len(rf.log)-1)
	rf.persist()

	return len(rf.log) - 1, term, isLeader
}

func (rf *Raft) Candidate() {

	rf.IncrementCurrentTerm()
	rf.SetVotedFor(int32(rf.me))
	rf.mu.Lock()
	rf.persist()
	rf.mu.Unlock()
	votes := 1

	args := &RequestVoteArgs{
		Term:         rf.GetCurrentTerm(),
		CandidateId:  int32(rf.me),
		LastLogTerm:  rf.GetLastLogEntry().Term,
		LastLogIndex: len(rf.log) - 1,
	}
	replyCh := make(chan RequestVoteReply, len(rf.peers))

	// send RequestVote RPCs to all other servers
	for id, peer := range rf.peers {
		if id != rf.me { // skip sending rpc to myself
			go func(peer *labrpc.ClientEnd, id int) {
				reply := &RequestVoteReply{}
				termSent := rf.GetCurrentTerm()
				if ok := peer.Call("Raft.RequestVote", args, reply); ok {
					// SGTR: guard against term confusion
					if rf.GetCurrentTerm() != termSent {
						return
					}
					reply.Id = id
					replyCh <- *reply
				} else {
					replyCh <- *reply
				}
			}(peer, id)
		}
	}

	for {
		select {
		case <-rf.appendEntriesCh:
			// we just received an appendEntries from the leader so we concede
			// our candidacy and revert to followers
			rf.SetServerState(Follower)
			return
		case <-rf.voteGrantedCh:
			rf.SetServerState(Follower)
			return

		case reply := <-replyCh:
			if reply.VoteGranted {
				votes++
				// if votes received from majority of servers: become leader
				if rf.IsMajority(votes) {
					rf.SetServerState(LeaderTransition)
					return
				}
			} else if reply.Term > rf.GetCurrentTerm() {
				// if the term returned by the peer server is higher than our
				// current term, we are not eligible to be leader. If their
				// current term is higher, that means we could have missing
				// committed entries from that term. So we update our current
				// term and become Followers.

				rf.SetVotedFor(-1)
				rf.SetServerState(Follower)
				rf.SetCurrentTerm(reply.Term)
				rf.mu.Lock()
				rf.persist()
				rf.mu.Unlock()
				return
			}

		case <-RandomTimeout(400, 600):
			// election timed out. A new election will start after
			// we return to ticker() in the same Candidate state.
			return
		}
	}

}

func CheckUpToDate(log1Term, log1Index, log2Term, log2Index int) bool {
	if log1Term > log2Term {
		return true
	}
	if log1Term == log2Term {
		return log1Index >= log2Index
	}
	return false
}

func (rf *Raft) CanCommit(n int) bool {
	numReplicated := 0
	for i := 0; i < len(rf.matchIndex); i++ {
		idx := rf.GetMatchIndex(i)
		if int(idx) >= n {
			numReplicated++
		}
	}
	if numReplicated < (len(rf.peers)/2)+1 {
		return false
	}
	entry, ok := rf.GetLogEntry(n)
	if !ok {
		log.Fatalf("(cancommit) Attempted to index invalid log entry: %d", n)
	}
	if entry.Term != int(rf.GetCurrentTerm()) {
		return false
	}
	return true
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		for rf.GetCommitIndex() > rf.GetLastApplied() {
			rf.IncrementLastApplied()
			lastApplied := rf.GetLastApplied()
			entry, ok := rf.GetLogEntry(lastApplied)
			if !ok {
				log.Fatalf("(%d ticker) Attempted to index invalid log entry: %d >= %d", rf.me, lastApplied, len(rf.log))
			}
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.GetLastApplied(),
			}
		}

		switch rf.GetServerState() {
		case Follower:
			select {
			case <-rf.appendEntriesCh:
				// if we get a message to reset timeout, just refresh by doing nothing
				// and going into the next iteration of the loop
				continue
			case <-rf.voteGrantedCh:
				// (Student Guide) reset timer if I grant a vote
				continue
			case <-RandomTimeout(400, 600):
				// if the election timeout occurs, convert to candidate
				rf.SetServerState(Candidate)
			}

		case Candidate:
			rf.Candidate()

		case LeaderTransition:
			for i := 0; i < len(rf.peers); i++ {
				rf.SetNextIndex(i, rf.GetLastLogIndex()+1)
				if i == rf.me {
					rf.SetMatchIndex(i, rf.GetLastLogIndex())
				} else {
					rf.SetMatchIndex(i, 0)
				}
			}
			rf.SetServerState(Leader)

		case Leader:

			for id, peer := range rf.peers {
				if id != rf.me {
					var args *AppendEntriesArgs
					if rf.GetLastLogIndex() >= rf.GetNextIndex(id) { // actually append entries
						prevLogIndex := rf.GetNextIndex(id) - 1
						prevLogEntry, ok := rf.GetLogEntry(prevLogIndex)
						if !ok {
							log.Fatalf("(appending) Attempted to index invalid log entry: %d", prevLogIndex)
						}

						entries := rf.GetLogSuffix(prevLogIndex + 1)
						entriesCopy := make([]*LogEntry, 0)
						for _, entry := range entries {
							copied := *entry
							entriesCopy = append(entriesCopy, &copied)
						}

						args = &AppendEntriesArgs{
							Term:         rf.GetCurrentTerm(),
							LeaderId:     rf.me,
							PrevLogIndex: prevLogIndex,
							PrevLogTerm:  prevLogEntry.Term,
							Entries:      entriesCopy,
							LeaderCommit: rf.GetCommitIndex(),
						}
					} else { // just a heartbeat message
						prevLogIndex := rf.GetNextIndex(id) - 1
						prevLogEntry, ok := rf.GetLogEntry(prevLogIndex)
						if !ok {
							log.Fatalf("(heartbeat) Attempted to index invalid log entry: %d", prevLogIndex)
						}
						args = &AppendEntriesArgs{
							Term:         rf.GetCurrentTerm(),
							LeaderId:     rf.me,
							PrevLogIndex: prevLogIndex,
							PrevLogTerm:  prevLogEntry.Term,
							Entries:      []*LogEntry{},
							LeaderCommit: rf.GetCommitIndex(),
						}
					}

					go func(peer *labrpc.ClientEnd, id int, args *AppendEntriesArgs) {
						reply := &AppendEntriesReply{
							Success:     false,
							NumAppended: 0,
						}
						termSent := rf.GetCurrentTerm()
						ok := peer.Call("Raft.AppendEntries", args, reply)
						if ok {
							// SGTR: guard against term confusion
							if rf.GetCurrentTerm() != termSent {
								return
							}

							if reply.Term > rf.GetCurrentTerm() {
								rf.SetCurrentTerm(reply.Term)
								rf.SetVotedFor(-1)
								rf.SetServerState(Follower)
								rf.mu.Lock()
								rf.persist()
								rf.mu.Unlock()
								return
							}

							if reply.Success {
								rf.SetNextIndex(id, rf.GetLastLogIndex()+1)

								rf.SetMatchIndex(id, args.PrevLogIndex+len(args.Entries))
								n := rf.GetLastLogIndex()
								for n > rf.GetCommitIndex() {
									if rf.CanCommit(n) {
										rf.SetCommitIndex(n)
										return
									}
									n--
								}
							} else {
								// if reply failed because the leader's term is too low,
								// then we should just discard this response
								if reply.Term <= rf.GetCurrentTerm() {
									rf.SetNextIndex(id, reply.ConflictIndex)
								}
							}
						}
					}(peer, id, args)
				}
			}

			time.Sleep(100 * time.Millisecond)

		default:
			log.Fatalf("Something went very wrong. State is %v", rf.GetServerState())
		}
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

	// dont need to use any concurrency primitives since
	// no other goroutines accessing or modifying the
	// peer's state at this point
	rf.currentTerm = 0
	rf.votedFor = -1 // -1 represents null
	rf.log = make([]*LogEntry, 0)
	rf.log = append(rf.log, &LogEntry{Term: 0, Command: nil})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int32, len(peers))
	rf.matchIndex = make([]int32, len(peers))

	rf.state = Follower
	rf.appendEntriesCh = make(chan struct{}, 10)
	rf.voteGrantedCh = make(chan struct{}, 10)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
