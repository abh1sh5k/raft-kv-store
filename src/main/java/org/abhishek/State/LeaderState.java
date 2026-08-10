package org.abhishek.State;

import common.LogEntry;
import org.abhishek.entity.Command;
import org.abhishek.entity.LogEntries;
import org.abhishek.entity.RaftNode;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.*;

public class LeaderState implements State{

    private RaftNode raftNode;
    private ScheduledExecutorService timerScheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> electionTimer;
    private boolean isCurrentStateValid;
    private long lastCommittedOffset;
    private long currentTerm;
    private long lastLogOffset;
    private long lastOffsetTerm;
    private final ConcurrentHashMap<Long, LogEntries> logQueue;
    private HashSet<Long> votedFor;

    public LeaderState(RaftNode raftNode)
    {
        this.raftNode = raftNode;
        this.lastCommittedOffset = raftNode.getLastLogOffset();
        this.currentTerm = raftNode.getCurrentTerm();
        this.lastLogOffset = raftNode.getLastLogOffset();
        this.lastOffsetTerm = raftNode.getLastOffsetTerm();
        this.logQueue = raftNode.getLogQueue();
        isCurrentStateValid = true;

    }

    public void onEnterState(){

    }

    public void onExitState(){

    }

    public boolean onAppendEntry(long term, long leaderId, long prevLogTerm, long prevLogIndex, List<LogEntry> entries, long leaderCommit){
        return true;
    }

    public boolean onRequestVote(long term, long logIndex, long lastTerm){
        return true;
    }

    public CompletableFuture<Boolean> handleClientRequest(Command cmd){
    return true;
    }
}
