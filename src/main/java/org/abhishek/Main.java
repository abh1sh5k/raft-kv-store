package org.abhishek;

import raft.Raft;
import common.Common;

public class Main {

    public static void main(String[] args) {

    }
}

/*

Dir Structure
- Raft
    - RaftServer
    - RaftClient
- Node
    - RaftNode

- RaftNode will create the connections
- It will have the peer list
- It will receive the connection requests
- Node will have the role defined and it will trigger the raft service accordingly
- For different role we will have different responses each for different APIs call. Let say we receive the KV store
  requests from the client we will send the output based on the role.
  Also I want to integrate different functionality based on the role assigned, in an elegant way.

 */