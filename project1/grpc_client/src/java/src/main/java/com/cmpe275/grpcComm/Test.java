package com.cmpe275.grpcComm;

import com.cmpe275.grpcComm.JavaAPI;

import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;


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

// to run this java file, use this command:
// mvn clean package
// java -cp target/grpcJava-1.0-SNAPSHOT-jar-with-dependencies.jar com.cmpe275.grpcComm.JavaClient


public class Test {
    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers.newFor("Weather Data").build().defaultHelp(true)
                .description("Weather Data Lake Java API v1.0");
        parser.addArgument("-H", "--host").type(String.class).setDefault("0.0.0.0").help("The host of the grpc server");
        parser.addArgument("-P", "--port").type(Integer.class).setDefault(8080)
                .help("The port listened by grpc server");
        parser.addArgument("-f", "--file").type(String.class).setDefault("../mesowesteasy.out")
                .help("The file path to upload");
        parser.addArgument("-g", "--get").action(Arguments.storeTrue()).setDefault(false)
                .help("-g -t <from_utc> <to_utc>");
        parser.addArgument("-u", "--upload").action(Arguments.storeTrue()).setDefault(false)
                .help("Upload data to the server");
        parser.addArgument("-p", "--ping").action(Arguments.storeTrue()).setDefault(false).help("Ping the server");
        parser.addArgument("-t", "--range").type(String.class).nargs(2).help("-t <from_utc> <to_utc>");
        parser.addArgument("-s", "--stations").nargs("*").help("-s <station1> <station2> <...>");
        parser.addArgument("-m", "--message").type(String.class).setDefault("Hello World!").help("-m 'Hello World!'");
        parser.addArgument("-o", "--output").type(String.class).setDefault("./result.out")
                .help("-m 'Specify the output file locaton for queries'");
        parser.addArgument("-i", "--sender").type(String.class).setDefault("0.0.0.0")
                .help("-m 'The sender IP address in format: x.x.x.x'");
        parser.addArgument("-b", "--broadcast").action(Arguments.storeTrue()).setDefault(false).help("-m 'Put to all nodes if True, otherwise just put to the given host'");
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }

        String host = ns.getString("host");
        Integer port = ns.getInt("port");
        String sender = ns.getString("sender");

        try {
            //System.out.println("start tring...");
			if(ns.getBoolean("get")) {
				List<String> range = ns.getList("range");
				String from_utc = range.get(0);
				String to_utc = range.get(1);
                JavaAPI.getAPI(ns.getString("output"), from_utc, to_utc, host, port, sender,"[{\"lhs\": \"STN\", \"op\": \"eq\", \"rhs\": [\"HCOT1\", \"BBN\"]}]");
            } else if (ns.getBoolean("ping")) {
                //System.out.println("start ping...");
                JavaAPI.pingAPI(ns.getString("message"), host, port, sender);
            } else if (ns.getBoolean("upload")) {
                String fp = ns.getString("file");
                boolean b = ns.getBoolean("broadcast");
                JavaAPI.putAPI(fp, b, host, port, sender);
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}

