package org.abhishek.State;

import common.LogEntry;
import org.abhishek.entity.Cmd;
import org.abhishek.entity.Command;
import org.abhishek.entity.RaftNode;
import org.abhishek.entity.Role;

import java.util.List;
import java.util.concurrent.*;

public class FollowerState implements State{

    private RaftNode raftNode;
    private ScheduledExecutorService timerScheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> electionTimer;
    private boolean isCurrentStateValid;
    private long lastCommittedOffset;
    private long currentTerm;
    private long logOffset;

    public FollowerState(RaftNode raftNode)
    {
        this.raftNode = raftNode;
        this.lastCommittedOffset = raftNode.getLastLogOffset();
        this.currentTerm = raftNode.getCurrentTerm();
        this.logOffset = raftNode.getLastLogOffset();
        isCurrentStateValid = true;
    }

    @Override
    public void onEnterState()
    {

        raftNode.setRole(Role.FOLLOWER);
        isCurrentStateValid = true;
        resetElectionTimer();

    }

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
        raftNode.transitionTo(this, new CandidateState(raftNode));
    }

    @Override
    public synchronized void onExitState()
    {

        electionTimer.cancel(true);
        this.isCurrentStateValid = false;

    }

    private void addEntry(List<LogEntry> entries)
    {
        for(LogEntry log: entries)
        {

        }
    }

    @Override
    public boolean onAppendEntry(long term, long leaderId, long prevLogTerm, long prevLogIndex, List<LogEntry> entries, long leaderCommit)
    {

        if(currentTerm > prevLogTerm)
        {
            return false;
        }
        else {
            currentTerm = term;
            if(prevLogIndex==logOffset)
            {
                addEntry(entries);
                return true;
            }
            return false;
        }

    }

    public CompletableFuture<Boolean> handleClientRequest(Command cmd)
    {
        return CompletableFuture.failedFuture(new RuntimeException("Not a Leader"));
    }

    @Override
    public boolean onRequestVote(long logIndex, long lastTerm)
    {
        if(lastTerm == currentTerm)
        {
            if(logIndex >= lastCommittedOffset)
                return true;
            else
            {
                return false;
            }
        }
        else if (lastTerm > currentTerm){
            return true;
        }
        else return false;
    }


}


/*



 */