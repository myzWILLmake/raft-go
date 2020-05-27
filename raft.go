package raft

import (
	crand "crypto/rand"
	"fmt"
	"math/big"
	"math/rand"
	"sync"
	"time"
)

const AppendEntriesInterval = time.Duration(100 * time.Millisecond)
const ElectionTimeout = time.Duration(1000 * time.Millisecond)

func init() {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	seed := bigx.Int64()
	rand.Seed(seed)
}

func newRandDuration(min time.Duration) time.Duration {
	extra := time.Duration(rand.Int63()) % min
	return time.Duration(min + extra)
}

type Raft struct {
	mu            sync.Mutex
	peers         []peerWrapper
	me            int
	leaderId      int
	currentTerm   int
	logIndex      int
	votedFor      int
	commitIndex   int
	lastApplied   int
	state         raftState
	logs          []LogEntry
	nextIndex     []int
	matchIndex    []int
	electionTimer *time.Timer

	clientCh chan ClientMsg
	notifyCh chan struct{}
}

func (rf *Raft) resetElectionTimer(duration time.Duration) {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(duration)
}

func (rf *Raft) initIndex() {
	peersNum := len(rf.peers)
	rf.nextIndex = make([]int, peersNum)
	rf.matchIndex = make([]int, peersNum)
	for i := 0; i < peersNum; i++ {
		rf.nextIndex[i] = rf.logIndex
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
	rf.leaderId = -1

	rf.resetElectionTimer(newRandDuration(ElectionTimeout))
}

func (rf *Raft) notifyNewLeader() {
	rf.clientCh <- ClientMsg{false, -1, -1, "NewLeader"}
}

func (rf *Raft) canCommit(index int) bool {
	if index < rf.logIndex && rf.commitIndex < index && rf.logs[index].LogTerm == rf.currentTerm {
		majority, count := len(rf.peers)/2+1, 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= index {
				count++
			}
		}
		return count >= majority
	} else {
		return false
	}
}

func (rf *Raft) requestLeader(server int, args RequestVoteArgs, replyCh chan RequestVoteReply) {
	var reply RequestVoteReply
	if err := rf.peers[server].Call("Raft.RequestVote", &args, &reply); err != nil {
		reply.Err, reply.Server = "RPC Failed", server
	}

	replyCh <- reply
}

func (rf *Raft) campaign() {
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}

	rf.leaderId = -1
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	args := RequestVoteArgs{rf.currentTerm, rf.me, rf.logIndex - 1, rf.currentTerm}
	electionDuration := newRandDuration(ElectionTimeout)
	rf.resetElectionTimer(electionDuration)
	timer := time.After(electionDuration)
	rf.mu.Unlock()

	replyCh := make(chan RequestVoteReply, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.requestLeader(i, args, replyCh)
		}
	}
	voteCount := 1
	majority := len(rf.peers)/2 + 1
	for voteCount < majority {
		select {
		case <-timer:
			return
		case reply := <-replyCh:
			if reply.Err != "" {
				go rf.requestLeader(reply.Server, args, replyCh)
			} else if reply.VoteGranted {
				voteCount++
			} else {
				rf.mu.Lock()
				if rf.currentTerm < reply.Term {
					rf.becomeFollower(reply.Term)
					rf.mu.Unlock()
				}
			}
		}
	}

	rf.mu.Lock()
	if rf.state == Candidate {
		fmt.Printf("NODE %d becomes a new leader\n", rf.me)
		rf.state = Leader
		rf.initIndex()
		go rf.heartbeat()
		go rf.notifyNewLeader()
	}

	rf.mu.Unlock()
}

func (rf *Raft) sendLogEntry(follower int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.nextIndex[follower] - 1
	prevLogTerm := rf.logs[prevLogIndex].LogTerm
	args := AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, rf.commitIndex, []LogEntry{}}
	if rf.nextIndex[follower] < rf.logIndex {
		entries := rf.logs[prevLogIndex+1 : rf.logIndex]
		args.Entries = entries
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	if err := rf.peers[follower].Call("Raft.AppendEntries", &args, &reply); err == nil {
		rf.mu.Lock()
		if !reply.Success {
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
			} else {
				rf.nextIndex[follower] = Max(1, Min(reply.ConflictIndex, rf.logIndex))
			}
		} else {
			prevLogIndex := args.PrevLogIndex
			entriesLen := len(args.Entries)
			if prevLogIndex+entriesLen >= rf.nextIndex[follower] {
				rf.nextIndex[follower] = prevLogIndex + entriesLen + 1
				rf.matchIndex[follower] = prevLogIndex + entriesLen
			}
			toCommitIndex := prevLogIndex + entriesLen
			if rf.canCommit(toCommitIndex) {
				rf.commitIndex = toCommitIndex
				// persist
				rf.notifyCh <- struct{}{}
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartbeat() {
	timer := time.NewTimer(AppendEntriesInterval)
	for {
		select {
		case <-timer.C:
			if _, isLeader := rf.getState(); !isLeader {
				return
			}
			go rf.replicate()
			timer.Reset(AppendEntriesInterval)
		}
	}
}

func (rf *Raft) replicate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.sendLogEntry(i)
		}
	}
}

func (rf *Raft) getState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}

	index := rf.logIndex
	entry := LogEntry{index, rf.currentTerm, command}
	rf.logs = append(rf.logs, entry)
	rf.matchIndex[rf.me] = rf.logIndex
	rf.logIndex += 1
	//persist
	go rf.replicate()
	return index, rf.currentTerm, true
}

// func (rf *Raft) Kill() {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	rf.state = Follower
// 	close(rf.shutdown)
// }

func (rf *Raft) apply() {
	for {
		select {
		case <-rf.notifyCh:
			rf.mu.Lock()
			var cmdValid bool
			var entries []LogEntry
			cmdValid = true
			entries = rf.logs[rf.lastApplied+1 : rf.commitIndex+1]
			rf.lastApplied = rf.commitIndex
			// rf.persist
			rf.mu.Unlock()
			for _, entry := range entries {
				rf.clientCh <- ClientMsg{cmdValid, entry.LogIndex, entry.LogTerm, entry.Command}

			}
		}
	}
}

func (rf *Raft) getServerInfo() map[string]interface{} {
	info := make(map[string]interface{})
	rf.mu.Lock()
	defer rf.mu.Unlock()

	info["id"] = rf.me
	info["leaderId"] = rf.leaderId
	info["currentTerm"] = rf.currentTerm
	info["voteFor"] = rf.votedFor
	info["commitIndex"] = rf.commitIndex
	info["lastApplied"] = rf.lastApplied
	info["logIndex"] = rf.logIndex
	info["state"] = rf.state
	if rf.logIndex > 6 {
		info["logs"] = rf.logs[len(rf.logs)-5:]
	} else {
		info["logs"] = rf.logs[1:rf.logIndex]
	}
	return info
}

func MakeRaft(peers []peerWrapper, me int, clientCh chan ClientMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me
	rf.leaderId = -1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.logIndex = 1
	rf.state = Follower
	rf.logs = []LogEntry{{0, 0, nil}}
	rf.clientCh = clientCh
	rf.notifyCh = make(chan struct{}, 1024)
	rf.electionTimer = time.NewTimer(newRandDuration(ElectionTimeout))

	go rf.apply()
	go func() {
		for {
			select {
			case <-rf.electionTimer.C:
				rf.campaign()
			}
		}
	}()

	return rf
}
