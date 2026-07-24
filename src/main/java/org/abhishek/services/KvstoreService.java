/*
   In this phase we are dealing with single node and what we are doing is when client requests a
   command we will add the entry to blockingQueue and send ack directly to the client
   In next phase we will add the part where we asks the follower to append the
   entries and gave ack when majority has appended it to their logs and increased the offset
   then we will commit the offset.
*/
package org.abhishek.services;

import io.grpc.stub.StreamObserver;
import kvstore.*;
import org.abhishek.State.State;
import org.abhishek.entity.DeleteTask;
import org.abhishek.entity.Entry;
import org.abhishek.entity.PutTask;
import org.abhishek.entity.RaftNode;
import org.abhishek.kvstore.KvStoreMachine;

import java.util.concurrent.CompletableFuture;

public class KvstoreService extends kvstoreGrpc

        .kvstoreImplBase {

    private final RaftNode raftNode;
    private final KvStoreMachine kvStoreMachine;
    private final State raftState;

    // Add KvStoreMachine object and use dependency injection in this

    public KvstoreService(RaftNode raftNode)
    {
        this.raftNode = raftNode;
        this.raftState = raftNode.getRaftState();
        this.kvStoreMachine = raftNode.getKvStoreMachine();
    }

    public void addEntriestoKVmachine(Entry entry) throws InterruptedException {
            kvStoreMachine.addEntryToQueue(entry);
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {

        /*
            Handle Client Requests
            // Based on role
        */
        // Trigger the append entry part in the state pattern
        raftState.onEntryAdd();

        CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();
        Entry entry = new PutTask(request.getKey(), request.getValue(), completableFuture);
        try {
            kvStoreMachine.addEntryToQueue(entry);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        PutResponse response = PutResponse.newBuilder().setSuccess(true).build();
        handleAsyncResponse(completableFuture, responseObserver, response);
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {

        /*
            Handle Clients Requests Based on Role

        */
        raftState.onEntryAdd();
        CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();
        Entry entry = new DeleteTask(request.getKey(), completableFuture);
        try {
            kvStoreMachine.addEntryToQueue(entry);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        DeleteResponse response = DeleteResponse.newBuilder().setSuccess(true).build();
        handleAsyncResponse(completableFuture, responseObserver, response);
    }

    public <T> void handleAsyncResponse(CompletableFuture<Boolean> completableFuture, StreamObserver<T> responseObserver, T responseInstance) {
        completableFuture.whenComplete((success, ex) -> {
            if (ex != null) {
                responseObserver.onError(
                        io.grpc.Status.NOT_FOUND.withDescription(ex.getMessage()).asRuntimeException()
                );
                return;
            }
            if (success) {
                responseObserver.onNext(responseInstance);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(io.grpc.Status.INTERNAL.asRuntimeException());
            }
        });
    }
}
