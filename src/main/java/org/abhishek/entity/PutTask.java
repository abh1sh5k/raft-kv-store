package org.abhishek.entity;

import java.util.concurrent.CompletableFuture;

public record PutTask(
    String key,
    String value,
    CompletableFuture<Boolean> completionFuture) implements Entry{
    @Override
    public CompletableFuture<Boolean> getCompletionFuture(){
        return completionFuture;
    }}
