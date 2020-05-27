package raft

func Min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func Max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

type raftState int

const (
	Leader raftState = iota
	Follower
	Candidate
)

type LogEntry struct {
	LogIndex int
	LogTerm  int
	Command  interface{}
}

type RequestVoteArgs struct {
	Term,
	CandidateId,
	LastLogIndex,
	LastLogTerm int
}

type RequestVoteReply struct {
	Err         string
	Server      int
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term,
	LeaderId,
	PrevLogIndex,
	PrevLogTerm,
	LeaderCommit int
	Entries []LogEntry
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
}

type ClientMsg struct {
	CmdValid bool
	CmdIndex int
	CmdTerm  int
	Cmd      interface{}
}

func getStateStr(state raftState) string {
	var rfStateStr string
	switch state {
	case Leader:
		rfStateStr = "Leader"
	case Follower:
		rfStateStr = "Follower"
	case Candidate:
		rfStateStr = "Candidate"
	}
	return rfStateStr
}
