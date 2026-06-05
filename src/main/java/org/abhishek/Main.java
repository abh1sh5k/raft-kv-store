package org.abhishek;

import org.abhishek.entity.RaftNode;
import java.util.Collections;

public class Main {
    public static void main(String[] args) {
        System.out.println("[DIAGNOSTIC 1] Inside Main method start.");
        try {
            RaftNode node = new RaftNode(9999, Collections.emptyList());

            System.out.println("[DIAGNOSTIC 2] About to invoke node.start().");
            node.start();

            System.out.println("[DIAGNOSTIC 5] node.start() has finished executing!");

        } catch (Exception e) {
            System.out.println("[DIAGNOSTIC ERROR] Caught exception in Main:");
            e.printStackTrace();
        }
        System.out.println("[DIAGNOSTIC 6] Reached very end of Main method. JVM will now exit!");
    }
}