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
        super.requestVote(request, responseObserver);
    }

    @Override
    public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> resonseObserver)
    {

    }
}
