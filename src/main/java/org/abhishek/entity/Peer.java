package org.abhishek.entity;

import io.grpc.ManagedChannel;
import raft.RaftServiceGrpc;

public record Peer(int port, String address, ManagedChannel channel, RaftServiceGrpc.RaftServiceBlockingStub stub) {
}
