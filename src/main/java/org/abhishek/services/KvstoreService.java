package org.abhishek.services;

import org.abhishek.entity.RaftNode;

import java.util.Map;

public class KvstoreService {

    private Map<String, String> kvstore;
    public KvstoreService(RaftNode raftNode)
    {
        kvstore = raftNode.kv
    }
}
