package org.abhishek.State;

import org.abhishek.entity.RaftNode;

public interface State{

    public void onEnterState();

    public void onExitState();

    public void onEntryAdd();

    public void onRequestVote();



}
