package org.abhishek;

import raft.Raft;
import common.Common;

public class Main {

    public static void main(String[] args) {

        Raft.VoteRequest request =
                Raft.VoteRequest.newBuilder()
                        .setTerm(1)
                        .setCandidateId(1)
                        .build();

        System.out.println(request.getTerm());
    }
}