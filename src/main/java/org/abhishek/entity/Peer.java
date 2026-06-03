package org.abhishek.entity;

import io.grpc.ManagedChannel;
import raft.RaftServiceGrpc;

public class Peer {

    final int port;
    final String address;
    final ManagedChannel channel;
    final RaftServiceGrpc.RaftServiceBlockingStub stub;

    public Peer(int port, String address, ManagedChannel channel, RaftServiceGrpc.RaftServiceBlockingStub stub) {
        this.port = port;
        this.address = address;
        this.channel = channel;
        this.stub = stub;
    }
}
