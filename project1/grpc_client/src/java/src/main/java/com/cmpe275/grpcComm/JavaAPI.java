package com.cmpe275.grpcComm;

import com.cmpe275.grpcComm.JavaClient;
import java.time.*;
import com.google.protobuf.ByteString;
import com.mashape.unirest.http.Unirest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;
import com.mashape.unirest.http.Unirest;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;


public class JavaAPI {
    private static final Logger logger = Logger.getLogger(JavaClient.class.getName());

    public static boolean pingAPI(String msg, String host, int port, String sender) {
        JavaClient client = new JavaClient(host, port, sender);
        return client.ping(msg);
    }

    public static void getAPI(String filePath, String fromUtc, String toUtc, String host, int port, String sender, String param_json) throws Exception {
        JavaClient client = new JavaClient(host, port, sender);
        String nodeString = Unirest.get("https://cmpe275-spring-18.mybluemix.net/get").asString().getBody();
        String[] nodes = nodeString.split(",");
        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            fw = new FileWriter(filePath);
            bw = new BufferedWriter(fw);
            if (client.get(bw, fromUtc, toUtc, param_json)) {
                logger.info("get succeeded at this node: " + client.receiver);
            } else {
                boolean isDone = false;
                for (String node : nodes) {
                    JavaClient client1 = new JavaClient(node, port, host);
                    if (client1.get(bw, fromUtc, toUtc, param_json)) {
                        logger.info("get succeeded at this node: " + client1.receiver);
                        isDone = true;
                        break;
                    }
                }
                if (!isDone) {
                    logger.info("failed at all nodes");
                }
            }
        } catch (IOException e) {
            logger.warning(e.getMessage());
        } finally {
            try {
                if (bw != null)
                    bw.close();
                if (fw != null)
                    fw.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public static boolean putAPI(String filePath, boolean isBroadcast, String host, int port, String sender) throws Exception {
        JavaClient client = new JavaClient(host, port, sender);
        String nodeString = Unirest.get("https://cmpe275-spring-18.mybluemix.net/get").asString().getBody();
        String[] nodes = nodeString.split(",");
        boolean res = true;
        if (!isBroadcast) {
           res = client.put(filePath);
        }
        if (isBroadcast || !res) {
            System.out.println("Now broadcasting...");
            for (String node : nodes) {
                if (node == host) {
                    continue;
                }
                try {
                    JavaClient client1 = new JavaClient(node, port, host);
                    if (client1.put(filePath)) {
                        logger.info("put succeeded at this node: " + client1.receiver);
                    }
                } catch (Exception e){
                    logger.info("put failed at all other nodes");
                    return false;
                } 
            }
        }
        return true;
    }
}


