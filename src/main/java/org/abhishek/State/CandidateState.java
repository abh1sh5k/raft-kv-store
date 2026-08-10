package org.abhishek.State;

import common.LogEntry;
import org.abhishek.entity.*;
import raft.RaftServiceGrpc;
import raft.VoteRequest;
import raft.VoteResponse;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CandidateState implements State{

    private RaftNode raftNode;
    private ScheduledExecutorService timerScheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> electionTimer;
    private boolean isCurrentStateValid;
    private long lastLogOffset;
    private long lastCommittedOffset;
    private long lastOffsetTerm;
    private long currentTerm;
    private volatile long lastAppliedOffset;
    private int id;

    public CandidateState(RaftNode raftNode)
    {
        this.id = raftNode.getId();
        this.raftNode = raftNode;
        isCurrentStateValid = true;
        this.lastCommittedOffset = raftNode.getLastLogOffset();
        this.currentTerm = raftNode.getCurrentTerm();
        this.lastLogOffset = raftNode.getLastLogOffset();
        this.lastOffsetTerm = raftNode.getLastOffsetTerm();
    }

    @Override
    public void onEnterState()
    {

        raftNode.setRole(Role.CANDIDATE);
        isCurrentStateValid = true;
        resetElectionTimer();

    }
    //properly sync this
    public synchronized void resetElectionTimer()
    {
        if (!isCurrentStateValid) return;
        if(electionTimer!=null)
            electionTimer.cancel(false);
        int timeout  = ThreadLocalRandom.current().nextInt(150, 300);
        electionTimer = timerScheduler.schedule(this::initiateElection, timeout, TimeUnit.MILLISECONDS);

    }

    public synchronized void initiateElection()
    {

        List<Peer> peers = raftNode.getPeers();
        this.currentTerm = currentTerm + 1;
        raftNode.setCurrentTerm(currentTerm);
        int majority = ((peers.size()+1)/2) + 1;
        AtomicInteger votesGranted = new AtomicInteger(1);
        resetElectionTimer();
        for(Peer peer: peers)
        {
            CompletableFuture.supplyAsync(() -> peer.stub().requestVote(
                            VoteRequest.newBuilder()
                                    .setCandidateId(id)
                                    .setTerm(currentTerm)
                                    .setLastLogIndex(lastLogOffset)
                                    .setLastLogTerm(lastOffsetTerm)
                                    .build()))
                    .thenAccept(response -> handleVoteResponse(response, votesGranted, majority))
                    .exceptionally(ex -> {
                        // peer unreachable/timed out — just doesn't count as a vote, election continues
                        return null;
                    });

        }
    }

    private synchronized void handleVoteResponse(VoteResponse response, AtomicInteger votesGranted, int majority)
    {
        if (!isCurrentStateValid) return;

        if (response.getTerm() > currentTerm) {
            raftNode.setCurrentTerm(response.getTerm());
            raftNode.transitionTo(this, new FollowerState(raftNode));
            return;
        }

        if (response.getVoteGranted() && votesGranted.incrementAndGet() >= majority) {
            raftNode.transitionTo(this, new LeaderState(raftNode));
        }
    }

    @Override
    public synchronized void onExitState()
    {

        electionTimer.cancel(true);
        this.isCurrentStateValid = false;

    }

    @Override
    public boolean onAppendEntry(long term, long leaderId, long prevLogTerm, long prevLogIndex, List<LogEntry> entries, long leaderCommit)
    {
        if (term >= currentTerm)
        {
            currentTerm = term;
            raftNode.transitionTo(this, new FollowerState(raftNode));
        }
        return false;
    }

    public CompletableFuture<Boolean> handleClientRequest(Command cmd)
    {
        return CompletableFuture.failedFuture(new RuntimeException("Election in Progress, Not a Leader"));
    }

    @Override
    public boolean onRequestVote(long term, long logIndex, long lastTerm)
    {
        if (term > currentTerm)
        {
            currentTerm = term;
            raftNode.transitionTo(this, new FollowerState(raftNode));
        }
        return false;
    }

}
