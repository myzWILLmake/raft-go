package raft

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Err, reply.Server = "", rf.me
	if rf.currentTerm == args.Term && rf.votedFor == args.CandidateId {
		reply.VoteGranted, reply.Term = true, rf.currentTerm
		return nil
	}

	if rf.currentTerm > args.Term ||
		(rf.currentTerm == args.Term && rf.votedFor != -1) {

		reply.VoteGranted, reply.Term = false, rf.currentTerm
		return nil
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		if rf.state != Follower {
			rf.resetElectionTimer(newRandDuration(ElectionTimeout))
			rf.state = Follower
		}
	}

	rf.leaderId = -1
	reply.Term = args.Term
	lastLogIndex := rf.logIndex - 1
	lastLogTerm := rf.logs[lastLogIndex].LogTerm
	if lastLogTerm > args.LastLogTerm ||
		(lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		reply.VoteGranted = false
		return nil
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.resetElectionTimer(newRandDuration(ElectionTimeout))
	// persist
	return nil
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term, reply.Success = rf.currentTerm, false
		return nil
	}

	reply.Term = args.Term
	rf.leaderId = args.LeaderId
	rf.resetElectionTimer(newRandDuration(ElectionTimeout))
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}

	rf.state = Follower
	logIndex := rf.logIndex
	prevLogIndex := args.PrevLogIndex
	if logIndex <= prevLogIndex || rf.logs[prevLogIndex].LogTerm != args.PrevLogTerm {
		conflictIdnex := Min(rf.logIndex-1, prevLogIndex)
		conflictTerm := rf.logs[conflictIdnex].LogTerm
		for ; conflictIdnex > rf.commitIndex && rf.logs[conflictIdnex-1].LogTerm == conflictTerm; conflictIdnex-- {
		}
		reply.Success, reply.ConflictIndex = false, conflictIdnex
		return nil
	}

	reply.Success, reply.ConflictIndex = true, -1
	rf.logs = rf.logs[:prevLogIndex+1]
	rf.logs = append(rf.logs, args.Entries...)
	rf.logIndex = prevLogIndex + len(args.Entries) + 1
	oldComitIndex := rf.commitIndex
	rf.commitIndex = Max(rf.commitIndex, Min(args.LeaderCommit, rf.logIndex-1))
	// persist()
	rf.resetElectionTimer(newRandDuration(ElectionTimeout))
	if rf.commitIndex > oldComitIndex {
		rf.notifyCh <- struct{}{}
	}
	return nil
}
