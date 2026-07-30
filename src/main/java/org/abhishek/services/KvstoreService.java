/*
   In this phase we are dealing with single node and what we are doing is when client requests a
   command we will add the entry to blockingQueue and send ack directly to the client
   In next phase we will add the part where we asks the follower to append the
   entries and gave ack when majority has appended it to their logs and increased the offset
   then we will commit the offset.
*/
package org.abhishek.services;

import common.Operation;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import kvstore.*;
import org.abhishek.State.State;
import org.abhishek.entity.Cmd;
import org.abhishek.entity.Command;
import org.abhishek.entity.RaftNode;
import org.abhishek.kvstore.KvStoreMachine;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class KvstoreService extends kvstoreGrpc

        .kvstoreImplBase {

    private State raftState;

    // Add KvStoreMachine object and use dependency injection in this

    public KvstoreService(State raftState)
    {
        this.raftState = raftState;
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {

        Command cmd = new Command(request.getKey(), request.getValue());
        CompletableFuture<Boolean> consensusFuture = raftState.handleClientRequest(cmd);

        consensusFuture.thenAccept(success -> {
            responseObserver.onNext(PutResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        }).exceptionally(ex -> {
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return null;
        });
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {

        /*
            Handle Clients Requests Based on Role

        */
        Command cmd = new Command(request.getKey());
        CompletableFuture<Boolean> consensusFuture = raftState.handleClientRequest(cmd);

        consensusFuture.thenAccept(success -> {
            responseObserver.onNext(DeleteResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        }).exceptionally(ex -> {
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return null;
        });
    }
}
