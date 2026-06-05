package org.abhishek.entity;

import java.util.concurrent.CompletableFuture;

public record DeleteTask(
        String key,
        CompletableFuture<Boolean> completionFuture) implements Entry{
    @Override
    public CompletableFuture<Boolean> getCompletionFuture(){
        return completionFuture;
    }}
