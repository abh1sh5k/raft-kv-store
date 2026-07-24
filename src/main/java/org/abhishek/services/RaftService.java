package org.abhishek.services;

import io.grpc.stub.StreamObserver;
import org.abhishek.entity.RaftNode;
import raft.*;

public class RaftService extends RaftServiceGrpc.RaftServiceImplBase{

    private final RaftNode raftNode;
    public RaftService(RaftNode raftNode)
    {
        this.raftNode = raftNode;
    }

    @Override
    public void requestVote(VoteRequest request, StreamObserver<VoteResponse> responseObserver) {

        //requestVote as Client -> only initiates in Candidate Role
        //requestVote as ServerProcessing -> Candidate votes for himself, Follower & Leader check the term if its greater than current Term it will cast its vote.

    }

    @Override
    public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> responseObserver)
    {
        //appendEntry Request -> will only be initiated by only Leader
        //appendEntry Response -> Follower will only respond with after appending it to the KVStore and update the LastCommitLog from leadersHeartBeat vote when logterm >= its current && logOffset>= its
        //handling purge when new leader is elected since its state dependent log have to find out a way for this
    }
}
