package org.abhishek.services;

import common.LogEntry;
import io.grpc.stub.StreamObserver;
import org.abhishek.State.State;
import org.abhishek.entity.RaftNode;
import raft.*;

import java.util.List;

public class RaftService extends RaftServiceGrpc.RaftServiceImplBase{

    private final RaftNode raftNode;
    private State raftState;
    public RaftService(RaftNode raftNode)
    {
        this.raftNode = raftNode;
        this.raftState = raftNode.getRaftState();
    }


    @Override
    public void requestVote(VoteRequest request, StreamObserver<VoteResponse> responseObserver) {

        //requestVote as Client -> only initiates in Candidate Role
        //requestVote as ServerProcessing -> Candidate votes for himself, Follower & Leader check the term if its greater than current Term it will cast its vote.


        boolean voteResponse = raftState.onRequestVote(request.getLastLogIndex(), request.getLastLogTerm());

        responseObserver.onNext(VoteResponse.newBuilder().setVoteGranted(voteResponse).setTerm(request.getTerm()).build());
        responseObserver.onCompleted();

    }

    @Override
    public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> responseObserver)
    {

        //appendEntry Request -> will only be initiated by only Leader
        //appendEntry Response -> Follower will only respond with after appending it to the KVStore and update the LastCommitLog from leadersHeartBeat vote when logterm >= its current && logOffset>= its
        //handling purge when new leader is elected since its state dependent log have to find out a way for this

        long term = request.getTerm();
        long leaderId = request.getLeaderId();
        long prevLogIndex = request.getPrevLogIndex();
        long prevLogTerm = request.getPrevLogTerm();
        List<LogEntry> entries = request.getEntriesList();
        long leaderCommit = request.getLeaderCommit();

        boolean response = raftState.onAppendEntry(term, leaderId, prevLogTerm, prevLogIndex, entries, leaderCommit);


    }
}
