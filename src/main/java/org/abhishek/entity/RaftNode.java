package org.abhishek.entity;

import com.google.rpc.context.AttributeContext;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.abhishek.services.KvstoreService;
import org.abhishek.services.RaftService;
import raft.RaftServiceGrpc;

import java.io.IOException;
import java.util.*;

public class RaftNode {

    private static int counter=1;
    private final int id;
    private final int port;

    private Map<String, String> kvStore;

    private List<Peer> peers;
    private Role role;

    private long currentTerm = 0;
    private Server grpcServer;

    public RaftNode(int port, List<Peer> peers)
    {
        id =counter++;
        this.port = port;
        this.peers = peers;

    }
    public Map<String, String> getKvStore(){
        return this.kvStore;
    }
    public void start() throws IOException {
        grpcServer = ServerBuilder.forPort(this.port)
                .addService(new RaftService(this))
                .addService(new KvstoreService(this))
                .build();
    }
}


