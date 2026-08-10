package org.abhishek.State;

import common.LogEntry;
import common.Operation;
import org.abhishek.entity.*;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.*;



public class FollowerState implements State{

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

    public FollowerState(RaftNode raftNode)
    {
        this.votedFor = new HashSet<>();
        this.raftNode = raftNode;
        this.lastCommittedOffset = raftNode.getLastLogOffset();
        this.currentTerm = raftNode.getCurrentTerm();
        this.lastLogOffset = raftNode.getLastLogOffset();
        this.lastOffsetTerm = raftNode.getLastOffsetTerm();
        this.logQueue = raftNode.getLogQueue();
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

    private void addEntry(List<LogEntry> entries, long prevLogIndex)
    {
        //logQueue.tailMap(prevLogIndex, false).clear()
        for(LogEntry log: entries)
        {
            Cmd cmd =null;
            if (log.getCommand().getOperation() == Operation.PUT){
            cmd = Cmd.PUT;
            }
            else if (log.getCommand().getOperation() == Operation.DELETE)
            {
                cmd = Cmd.DELETE;
            }
            else if(log.getCommand().getOperation() == Operation.NOOP){
                continue;
            }

            Command command = new Command
                    (cmd, log.getCommand().getKey(), log.getCommand().getValue());
            logQueue.put(log.getIndex(), new LogEntries(log.getTerm(), command));
            lastLogOffset = log.getIndex();
            lastOffsetTerm = log.getTerm();
        }
    }

    @Override
    public boolean onAppendEntry(long term, long leaderId, long prevLogTerm, long prevLogIndex, List<LogEntry> entries, long leaderCommit) {

        if (term < currentTerm)
        {
            return false;
        }
        else {
            currentTerm = term;
            if((logQueue.containsKey(prevLogIndex) && logQueue.get(prevLogIndex).term() == prevLogTerm) || prevLogIndex==0)
            {
                resetElectionTimer();
                if(!entries.isEmpty()){
                    addEntry(entries, prevLogIndex);
                }
                if (leaderCommit > raftNode.getCommittedOffset()) {
                    long lastNewEntryIndex = entries.isEmpty() ? prevLogIndex : entries.getLast().getIndex();
                    raftNode.setCommittedOffset(Math.min(leaderCommit, lastNewEntryIndex));
                }
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
    public boolean onRequestVote(long term, long logIndex, long lastTerm)
    {
        if(votedFor.contains(term)){
            return false;
        }

        if(term > currentTerm)
            currentTerm = term;
        else return false;

        if(lastOffsetTerm == lastTerm)
        {
            if (logIndex >= lastLogOffset){
                resetElectionTimer();
                votedFor.add(term);
                return true;
            }
            else {
                return false;
            }
        }
        else if (lastOffsetTerm < lastTerm){
            resetElectionTimer();
            votedFor.add(term);
            return true;
        }
        else return false;
    }


}


/*
Task to be done

- Create proper variable for the prev data (term and Offset) and proper owner for this
- RequestVote
    - Follower
        - Increase the currentTerm
        - compare with prevLogTerm in their queue and their prevLogOffset
    - AppendEntry
        - compare within Logs info from provided prevleaderLog data one and take it from there




 */