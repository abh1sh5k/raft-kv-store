package org.abhishek.State;

import common.LogEntry;
import org.abhishek.entity.*;
import raft.AppendEntriesRequest;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.*;

public class LeaderState implements State{

    private RaftNode raftNode;
    private ScheduledExecutorService timerScheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> heartbeatTimer;
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

      raftNode.setRole(Role.LEADER);
      isCurrentStateValid = true;
      initiateHeartbeat();
    }

    public synchronized void initiateHeartbeat()
    {
        if(!isCurrentStateValid) return;
        if(heartbeatTimer!=null) heartbeatTimer.cancel(false);

        heartbeatTimer = timerScheduler.schedule(this::sendHeartbeat, 50, TimeUnit.MILLISECONDS);
    }

    public synchronized void sendHeartbeat()
    {
        List<Peer> peers = raftNode.getPeers();

        for(Peer peer: peers)
        {
            peer.stub().appendEntries(AppendEntriesRequest.newBuilder()
                            .setTerm()
                            .setLeaderId()
                            .setPrevLogIndex()
                            .setPrevLogTerm()
                            .setLeaderCommit()
                            .setEntries()
                            .build())
        }
    }



    public void onExitState(){
        heartbeatTimer.cancel(true);
        this.isCurrentStateValid = false;
    }



    public boolean onAppendEntry(long term, long leaderId, long prevLogTerm, long prevLogIndex, List<LogEntry> entries, long leaderCommit){
        if (term >= currentTerm)
        {
            currentTerm = term;
            raftNode.transitionTo(this, new FollowerState(raftNode));
        }
        return false;
    }

    public boolean onRequestVote(long term, long logIndex, long lastTerm){
        if (term > currentTerm)
        {
            currentTerm = term;
            raftNode.transitionTo(this, new FollowerState(raftNode));
        }
        return false;
    }

    public CompletableFuture<Boolean> handleClientRequest(Command cmd){

        CompletableFuture<Boolean> future = new CompletableFuture<>();
        future.
    }
}
