package org.abhishek.kvstore;

import org.abhishek.entity.DeleteTask;
import org.abhishek.entity.Entry;
import org.abhishek.entity.PutTask;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

public class KvStoreMachine implements Runnable{

    private final Map<String, String> kvStore;
    private final LinkedBlockingQueue<Entry> logQueue;
    private volatile boolean running = true;

    @Override
    public void run(){
        // Add this explicit diagnostic before the loop even starts to prove the thread is alive!
        System.out.println("====== [CRITICAL BOOT] KVSTOREMACHINE THREAD RUN LOOP IS OFFICIALLY RUNNING ======");
        while(running){
            try {
                Entry entry = logQueue.take();
                switch(entry){
                    case PutTask put->{
                        System.out.println("State BEFORE: " + kvStore);

                        kvStore.put(put.key(), put.value());

                        System.out.println("State AFTER : " + kvStore);

                        entry.getCompletionFuture().complete(true);  // set the object of getCompletionFuture to true
                    }
                    case DeleteTask delete->{

                        if(kvStore.containsKey(delete.key())){
                        System.out.println("State BEFORE: " + kvStore);
                        kvStore.remove(delete.key());
                        System.out.println("State AFTER : " + kvStore);
                        entry.getCompletionFuture().complete(true);
                        }
                        else {
                            System.out.println("[ENGINE CORNER CASE] Key not found! Completing exceptionally...");

                            // Pass a clean message text payload back across the pipeline
                            entry.getCompletionFuture().completeExceptionally(
                                    new IllegalArgumentException("KEY_MISSING_ERROR")
                            );
                        }

                          // set the object of getCompletionFuture to true
                    }
                    default -> throw new IllegalStateException("Unexpected value: " + entry);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
    }

    public KvStoreMachine(Map<String, String> kvStore, LinkedBlockingQueue<Entry> logQueue)
    {
        this.kvStore = kvStore;
        this.logQueue = logQueue;
    }

    public void addEntryToQueue(Entry entry) throws InterruptedException {
        logQueue.put(entry);
    }
}
