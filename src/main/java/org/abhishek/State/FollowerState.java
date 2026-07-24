package org.abhishek.State;

import org.abhishek.entity.RaftNode;
import org.abhishek.entity.Role;

import java.util.concurrent.*;

public class FollowerState implements State{

    RaftNode raftNode;
    private ScheduledExecutorService timerScheduler = Executors.newSingleThreadScheduledExecutor();
    ScheduledFuture<?> electionTimer;
    boolean isCurrentStateValid;

    public FollowerState(RaftNode raftNode)
    {
        this.raftNode = raftNode;
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
//        raftNode.transitionTo(this, newCandidateState(raftNode));
    }

    @Override
    public synchronized void onExitState()
    {

        electionTimer.cancel(true);
        this.isCurrentStateValid = false;

    }

    @Override
    public void onEntryAdd()
    {



    }

    @Override
    public void onRequestVote()
    {

    }


}


/*



 */