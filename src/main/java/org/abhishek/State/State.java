package org.abhishek.State;

import common.LogEntry;
import org.abhishek.entity.Cmd;
import org.abhishek.entity.Command;
import org.abhishek.entity.RaftNode;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface State{

    public void onEnterState();

    public void onExitState();

    public boolean onAppendEntry(long term, long leaderId, long prevLogTerm, long prevLogIndex, List<LogEntry> entries, long leaderCommit);

    public boolean onRequestVote(long logIndex, long lastTerm);

    public CompletableFuture<Boolean> handleClientRequest(Command cmd);


}
