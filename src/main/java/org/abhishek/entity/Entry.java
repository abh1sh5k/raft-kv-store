package org.abhishek.entity;

import java.util.concurrent.CompletableFuture;

public sealed interface Entry permits PutTask, DeleteTask{

    CompletableFuture<Boolean> getCompletionFuture();

}

