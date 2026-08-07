package org.abhishek.kvstore;

import org.abhishek.entity.Cmd;
import org.abhishek.entity.Command;
import org.abhishek.entity.LogEntries;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class KvStoreMachine implements Runnable{

    private final Map<String, String> kvStore;
    private final ConcurrentHashMap<Long, LogEntries> logQueue;
    private volatile boolean running = true;
    private volatile long lastAppliedLog;
    private volatile long lastCommittedOffset;


    public KvStoreMachine(Map<String, String> kvStore, ConcurrentHashMap<Long, LogEntries> logQueue, long lastCommittedOffset)
    {
        this.kvStore = kvStore;
        this.logQueue = logQueue;
        this.lastAppliedLog = 0;
        this.lastCommittedOffset = lastCommittedOffset;
    }

    @Override
    public void run(){
        // Add this explicit diagnostic before the loop even starts to prove the thread is alive!
        System.out.println("====== [CRITICAL BOOT] KVSTOREMACHINE THREAD RUN LOOP IS OFFICIALLY RUNNING ======");
        while(running){
            if(lastAppliedLog < lastCommittedOffset)
            {
            lastAppliedLog = lastAppliedLog + 1;
            Command entry = logQueue.get(lastAppliedLog).command();
            switch(entry.cmd()) {
                case PUT -> {
                    System.out.println("State BEFORE: " + kvStore);

                    kvStore.put(entry.key(), entry.value());

                    System.out.println("State AFTER : " + kvStore);

                }
                case DELETE -> {

                    if (kvStore.containsKey(entry.key())) {
                        System.out.println("State BEFORE: " + kvStore);
                        kvStore.remove(entry.key());
                        System.out.println("State AFTER : " + kvStore);
                    } else {
                        System.out.println("[ENGINE CORNER CASE] Key not found! Completing exceptionally...");

                        // Pass a clean message text payload back across the pipeline
                        throw new IllegalArgumentException("KEY_MISSING_ERROR");
                    }

                    // set the object of getCompletionFuture to true
                }

                default -> throw new IllegalStateException("Unexpected value: " + entry);
                }
            }

        }
    }



}
