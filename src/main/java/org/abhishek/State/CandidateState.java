package org.abhishek.State;

import common.LogEntry;
import org.abhishek.entity.Cmd;
import org.abhishek.entity.Command;
import org.abhishek.entity.RaftNode;
import org.abhishek.entity.Role;

import java.util.List;
import java.util.concurrent.*;

public class CandidateState implements State{

    private RaftNode raftNode;
    private ScheduledExecutorService timerScheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> electionTimer;
    private boolean isCurrentStateValid;
    private int lastLogOffset;
    private int lastCommitOffset;
    private volatile int lastAppliedOffset;

    public CandidateState(RaftNode raftNode)
    {
        this.raftNode = raftNode;
        isCurrentStateValid = true;
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
        return false;
    }

    public CompletableFuture<Boolean> handleClientRequest(Command cmd)
    {
        return CompletableFuture.failedFuture(new RuntimeException("Election in Progress, Not a Leader"));
    }

    @Override
    public boolean onRequestVote(long logIndex, long lastTerm)
    {
        return false;
    }

}
