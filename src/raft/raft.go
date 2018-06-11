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

import "sync"
import "labrpc"

import "math/rand"
import "time"

// import "bytes"
// import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	_ = iota
	Follower
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	currentTerm int
	currentLeader int
	state int

	voteFor map[int]int

	resetElection chan bool

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	isleader = rf.state == Leader
	term = rf.currentTerm
	rf.mu.Unlock()

	return term, isleader
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	VoteTerm int
	VoteFor int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VotePeer int
	Term int
	VoteFor int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	voteTerm := args.VoteTerm
	voteFor := args.VoteFor

	reply.VotePeer = rf.me
	reply.Term = rf.currentTerm

	if voteTerm < rf.currentTerm {
		reply.VoteFor = 0
		return
	}

	rf.mu.Lock()
	if rf.state == Leader {
		reply.VoteFor = 0
	} else if peer, ok := rf.voteFor[voteTerm]; ok && peer != voteFor {
		reply.VoteFor = 0
	} else {
		rf.voteFor[voteTerm] = voteFor
		reply.VoteFor = voteFor
		reply.Term = voteTerm
	}
	rf.mu.Unlock()
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
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.

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
	rf.currentTerm = -1
	rf.currentLeader = 0
	rf.state = Follower
	rf.voteFor = make(map[int]int)

	rf.resetElection = make(chan bool, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.checkHeartbeat()

	return rf
}

func (rf *Raft) keepHeartbeat() {
	d := time.Duration(150) * time.Millisecond
	timer := time.NewTimer(d)

	for {
		<- timer.C

		if _, isleader := rf.GetState(); !isleader {
			break
		}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			var args AppendEntriesArgs
			var reply AppendEntriesReply

			args.Term = rf.currentTerm
			args.LeaderPeer = rf.me

			go rf.sendAppendEntries(i, args, &reply)
		}

		timer.Reset(d)
	}
}

func (rf *Raft) checkHeartbeat() {
	var timer *time.Timer
	var timeout int
	first := true

	for {
		timeout = 180 + rand.Intn(100)
		if !first {
			timer.Reset(time.Duration(timeout) * time.Millisecond)
		} else {
			timer = time.NewTimer(time.Duration(timeout) * time.Millisecond)
			first = false
		}

		reset := false
		for expired := false; !reset && !expired; {
			select {
			case <- timer.C:
				expired = true
			case <- rf.resetElection:
				if !expired && !timer.Stop() {
					<- timer.C
				}
				reset = true
			}
		}
		if reset {
			continue
		}

		if _, isleader := rf.GetState(); isleader {
			continue
		}

		rf.startElection()
	}
}

func (rf *Raft) becomeCandidate() bool {
	leader := rf.me

	rf.mu.Lock()
	newTerm := rf.currentTerm + 1
	p, ok := rf.voteFor[newTerm]
	if ok && p != rf.me {
		rf.mu.Unlock()
		return false
	}

	rf.currentTerm = newTerm
	rf.state = Candidate
	rf.voteFor[newTerm] = leader
	rf.mu.Unlock()

	return true

}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.currentLeader = rf.me
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
}

func (rf *Raft) startElection() {
	ok := rf.becomeCandidate()
	if !ok {
		return
	}

	leader := rf.me
	newTerm := rf.currentTerm
	votePeers := make(map[int]bool)
	votes := make(chan int, len(rf.peers))
	leaderAlive := make(chan int, 1)

	DPrintf("start election term %d, leader %d.\n", newTerm, leader)

	for i := 0; i < len(rf.peers); i++ {
		go func(i int) {
			if i == leader {
				votes <- leader
				return
			}

			var args RequestVoteArgs
			var reply RequestVoteReply

			args.VoteTerm = newTerm
			args.VoteFor = rf.me

			ok := rf.sendRequestVote(i, &args, &reply)
			if !ok {
				DPrintf("request vote failed.\n")
				return
			}

			if reply.Term != newTerm {
				leaderAlive <- reply.Term
			} else if reply.VoteFor == leader {
				votes <- reply.VotePeer
			}
		}(i)
	}

	leaderTerm := -1
	majority := false
	timeout := 1500 + rand.Intn(500)
	timer := time.NewTimer(time.Duration(timeout) * time.Millisecond)
	for finish := false; !finish; {
		select {
		case peer := <- votes:
			votePeers[peer] = true
			if len(votePeers) >= len(rf.peers) / 2 + 1 {
				finish = true
				majority = true
			}
		case <-timer.C:
			finish = true
		case term := <- leaderAlive:
			if leaderTerm == -1 || term > leaderTerm {
				leaderTerm = term
			}
		}
	}

	DPrintf("Receive votes %d for peer %d.\n", len(votePeers), leader)

	if majority {
		go rf.keepHeartbeat()

		rf.mu.Lock()
		rf.becomeLeader()
		rf.mu.Unlock()
	} else if leaderTerm >= newTerm {
		rf.mu.Lock()
		rf.becomeFollower(leaderTerm)
		rf.mu.Unlock()
	}
}


type AppendEntriesArgs struct {
	Term int
	LeaderPeer int
	LogIndex int
	Logs *[][]byte
}

type AppendEntriesReply struct{
	Peer int
	LastIndex int
}


func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	term := args.Term
	leader := args.LeaderPeer

	DPrintf("Receive appendentries from leader %d within term %d on peer %d.\n", leader, term, rf.me)

	rf.resetElection <- true

	rf.mu.Lock()
	rf.currentTerm = term
	rf.currentLeader = leader
	rf.state = Follower
	rf.mu.Unlock()
}


func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply * AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
