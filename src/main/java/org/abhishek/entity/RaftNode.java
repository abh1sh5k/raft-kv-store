package org.abhishek.entity;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.abhishek.kvstore.KvStoreMachine;
import org.abhishek.services.KvstoreService;
import org.abhishek.services.RaftService;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class RaftNode{

    private static int counter=1;
    private final int id;
    private final int port;

    private final Map<String, String> kvStore;
    private final KvStoreMachine kvStoreMachine;

    private List<Peer> peers;
    private Role role;

    private long currentTerm = 0;
    private Server grpcServer;
    private final LinkedBlockingQueue<Entry> logQueue;

    public RaftNode(int port, List<Peer> peers)
    {
        id =counter++;
        this.port = port;
        this.peers = peers;
        this.kvStore = new HashMap<>();
        this.role = Role.FOLLOWER;
        this.logQueue = new LinkedBlockingQueue<>(25);
        this.kvStoreMachine = new KvStoreMachine(this.kvStore, this.logQueue);

    }

    public Map<String, String> getKvStore() { return this.kvStore; }
    public LinkedBlockingQueue<Entry> getLogQueue() { return logQueue; }
    public KvStoreMachine getKvStoreMachine() { return kvStoreMachine; }

    public void start() throws Exception {
        System.out.println("\n=================================================");
        System.out.println("[BOOT] STEP 1: Spawning Dedicated Database Engine Thread...");
        System.out.println("=================================================");

        Thread engineThread = new Thread(this.kvStoreMachine);
        engineThread.setName("KvStoreEngine-Worker-Thread");
        engineThread.setDaemon(true);
        engineThread.start();

        System.out.println("[BOOT] STEP 2: Binding gRPC Server Service Interfaces...");
        Server grpcServer = ServerBuilder.forPort(this.port)
                .addService(new KvstoreService(this))
                .build();

        System.out.println("[BOOT] STEP 3: Initializing Netty Network Socket Listener...");
        grpcServer.start();

        System.out.println("RaftNode [1] successfully active on port: " + this.port);
        System.out.println("[DIAGNOSTIC AWAIT] Entering awaitTermination blocking state...");
        grpcServer.awaitTermination();
    }
}


