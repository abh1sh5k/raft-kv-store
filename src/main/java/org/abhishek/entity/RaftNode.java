package org.abhishek.entity;

import common.LogEntry;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.abhishek.State.FollowerState;
import org.abhishek.State.State;
import org.abhishek.kvstore.KvStoreMachine;
import org.abhishek.services.KvstoreService;
import org.abhishek.services.RaftService;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class RaftNode{

    private static int counter=1;
    private final int id;
    private final int port;

    private final Map<String, String> kvStore;
    private final KvStoreMachine kvStoreMachine;

    private List<Peer> peers;
    private Role role;

    private volatile long currentTerm = 0;
    private volatile long lastLogOffset = 0;
    private volatile long lastCommittedOffset = 0;
    private Server grpcServer;
    private final ConcurrentHashMap<Long, Command> logQueue;
    private State raftState;

    public long getCurrentTerm() {
        return currentTerm;
    }

    public RaftNode(int port, List<Peer> peers)
    {
        id = counter++;
        this.port = port;
        this.peers = peers;
        this.kvStore = new HashMap<>();
        this.role = Role.FOLLOWER;
        this.raftState = new FollowerState(this);
        this.logQueue = new ConcurrentHashMap<>();
        this.kvStoreMachine = new KvStoreMachine(this.kvStore, this.logQueue, this.lastCommittedOffset);

    }

    public long getLastLogOffset() {
        return lastLogOffset;
    }

    public long getCommittedOffset() {
        return lastCommittedOffset;
    }

    public State getRaftState()
    {
        return raftState;
    }

    public void setRaftState(State raftState) {
        this.raftState = raftState;
    }

    public void setRole(Role role) {
        this.role = role;
    }

    // Should I store the State in Node only and use that to trigger the from role
    public void transitionTo(State from, State to)
    {
        from.onExitState();
        setRaftState(to);
        to.onEnterState();
    }

    public Map<String, String> getKvStore() { return this.kvStore; }
    public ConcurrentHashMap<Integer, Command> getLogQueue() { return logQueue; }
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
                .addService(new KvstoreService(this.raftState))
                .addService(new RaftService(this))
                .build();

        System.out.println("[BOOT] STEP 3: Initializing Netty Network Socket Listener...");
        grpcServer.start();

        System.out.println("RaftNode [1] successfully active on port: " + this.port);
        System.out.println("[DIAGNOSTIC AWAIT] Entering awaitTermination blocking state...");
        grpcServer.awaitTermination();
    }
}


