
package com.cmpe275.grpcComm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
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

import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class JavaClient {

	final static int CONST_MEDIA_TYPE_TEXT = 1;

	final static int CONST_CHUNK_SIZE = 2;

	final CountDownLatch done = new CountDownLatch(1);

	final static String CONST_MESOWEST_HEADER = "STN YYMMDD/HHMM MNET SLAT SLON SELV TMPF SKNT DRCT GUST PMSL ALTI DWPF RELH WTHR P24I";

	private static final Logger logger = Logger.getLogger(JavaClient.class.getName());

	final static String MY_IP = "localhost";

	private final ManagedChannel channel;
	private final CommunicationServiceGrpc.CommunicationServiceStub stub;
	private final CommunicationServiceGrpc.CommunicationServiceBlockingStub blockingStub;

	private String sender = "";
	private String receiver = "";

	public JavaClient(String host, int port, String sender) {
		this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).build();
		this.stub = CommunicationServiceGrpc.newStub(channel);
		this.blockingStub = CommunicationServiceGrpc.newBlockingStub(channel);
		this.sender = sender;
		this.receiver = host;
		logger.info("Start client...");
	}

	public void shutdown() throws InterruptedException {
		this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
	}

	public boolean put(String fpath) {
		StreamObserver<Response> responseObserver = new StreamObserver<Response>() {
			@Override
			public void onNext(Response value) {
				logger.info(value.getMsg());
			}
			@Override
			public void onError(Throwable t) {
				t.printStackTrace();
				done.countDown();
			}
			@Override
			public void onCompleted() {
				logger.info("Completed");
				done.countDown();
			}
		};

		StreamObserver<Request> requestObserver = this.stub.putHandler(responseObserver);
		//logger.info("Stream UP!!!");
		try {
			boolean is_starts_reading = false;
			//boolean is_mesonet = false;
			int current_chunk_size = 0;

			File f = new File(fpath);
			FileReader fr = new FileReader(f);
			BufferedReader br = new BufferedReader(fr);
			StringBuffer sb = new StringBuffer();
			String line;
			//logger.info("Buffers UP!!!");
			while((line = br.readLine()) != null) {
				logger.info("line:" + line);
				if(String.join(" ",line.trim().split("\\s+")).equalsIgnoreCase(CONST_MESOWEST_HEADER)) {
					is_starts_reading = true;
					continue;
				}
				if (!is_starts_reading) {
					continue;
				}

				sb.append(line + "\n");
				current_chunk_size++;

				if(current_chunk_size == CONST_CHUNK_SIZE) {
					DatFragment datFragment = DatFragment.newBuilder().setData(ByteString.copyFromUtf8(sb.toString())).build();
					logger.info("Data: " + sb.toString());
					Request req = Request.newBuilder().setPutRequest(PutRequest.newBuilder().setDatFragment(datFragment).build()).build();
					requestObserver.onNext(req);

					current_chunk_size = 0;
					sb = new StringBuffer();
				}
				// else {
				// 	sb.append(line + "\n");
				// 	current_chunk_size++;
				// 	logger.info("chunk size: " + current_chunk_size);
				// 	logger.info("tempDataaa: " + sb.toString());
				// }
			}

			//THIS IF STATEMENT MIGHT NOT BE NECESSARY DEPEND ON HOW THE REST OF THE CLASS DESIGN THE PROCESS
			if(current_chunk_size > 0) {
				logger.info("Dataaa: " + sb.toString());
				DatFragment datFragment = DatFragment.newBuilder().setData(ByteString.copyFromUtf8(sb.toString()))
						.build();
				Request req = Request.newBuilder()
						.setPutRequest(PutRequest.newBuilder().setDatFragment(datFragment).build()).build();
				requestObserver.onNext(req);
			}

			br.close();
			fr.close();
			requestObserver.onCompleted();
			done.await();
		} catch (Exception e) {
			requestObserver.onError(e);
			logger.log(Level.WARNING, "RPC failed: {0}", e.getMessage());
			return false;
		}

		logger.info("putHandler DONE");
		return true;
	}

	public boolean get(BufferedWriter fp, String from_utc, String to_utc) {
		String uuid = UUID.randomUUID().toString();
		QueryParams queryParams = QueryParams.newBuilder().setFromUtc(from_utc).setToUtc(to_utc).build();
		MetaData metaData = MetaData.newBuilder().setUuid(uuid).build();
		GetRequest getRequest = GetRequest.newBuilder().setMetaData(metaData).setQueryParams(queryParams).build();
		Request req = Request.newBuilder().setFromSender(this.sender).setToReceiver(this.receiver).setGetRequest(getRequest).build();

		Iterator<Response> it;
		try {
			it = this.blockingStub.getHandler(req);
			while(it.hasNext()) {
				Response data = it.next();
				if (data.getCodeValue() == 2) {
					System.out.println("read failed at this node!");
					return false;
				} else {
					ByteString byteStr = data.getDatFragment().getData();
					String str = byteStr.toStringUtf8();
					try {
						fp.write(str);
					} catch (IOException e ){
						System.out.println("Cannot write!");
						return false;
					}
				}
			}
		} catch (StatusRuntimeException e) {
			logger.log(Level.WARNING, "RPC failed", e);
			//System.out.println("Cannot get the iterator!");
			return false;
		}

		return true;
	}


	public boolean ping(String msg) {
		PingRequest pingRequest = PingRequest.newBuilder().setMsg(msg).build();
		Request request = Request.newBuilder().setFromSender(this.sender).setToReceiver(this.receiver)
				.setPing(pingRequest).build();
		Response response;
		try {
			response = this.blockingStub.ping(request);
			logger.info("Code: " + response.getCode());
			logger.info("Msg: " + response.getMsg());
			return true;
		} catch (StatusRuntimeException e) {
			logger.log(Level.WARNING, "RPC failed", e);
			return false;
		}
	}

	public static void main(String[] args) throws Exception {

		String nodeString = Unirest.get("https://cmpe275-spring-18.mybluemix.net/get").asString().getBody();
		String[] nodes = nodeString.split(",");

		ArgumentParser parser = ArgumentParsers.newFor("Weather Data").build()
				.defaultHelp(true)
				.description("Weather Data Lake Java API v1.0");
		parser.addArgument("-H","--host").type(String.class).setDefault("0.0.0.0").help("The host of the grpc server");
		parser.addArgument("-P","--port").type(Integer.class).setDefault(8080).help("The port listened by grpc server");
		parser.addArgument("-f","--file").type(String.class).setDefault("../mesowesteasy.out").help("The file path to upload");
		parser.addArgument("-g","--get").action(Arguments.storeTrue()).setDefault(false).help("-g -t <from_utc> <to_utc>");
		parser.addArgument("-u","--upload").action(Arguments.storeTrue()).setDefault(false).help("Upload data to the server");
		parser.addArgument("-p","--ping").action(Arguments.storeTrue()).setDefault(false).help("Ping the server");
		parser.addArgument("-t","--range").type(String.class).nargs(2).help("-t <from_utc> <to_utc>");
		parser.addArgument("-s","--stations").nargs("*").help("-s <station1> <station2> <...>");
		parser.addArgument("-m","--message").type(String.class).setDefault("Hello World!").help("-m 'Hello World!'");
		parser.addArgument("-o","--output").type(String.class).setDefault("./result.out").help("-m 'Specify the output file locaton for queries'");

		Namespace ns = null;
		try {
			ns = parser.parseArgs(args);
		} catch (ArgumentParserException e) {
			parser.handleError(e);
			System.exit(1);
		}
		String host = ns.getString("host");
		Integer port = ns.getInt("port");
		JavaClient client = new JavaClient(host, port, MY_IP);
		try {
			if(ns.getBoolean("get")) {
				List<String> range = ns.getList("range");
				String from_utc = range.get(0);
				String to_utc = range.get(1);
				BufferedWriter bw = null;
				FileWriter fw = null;
				try {
					fw = new FileWriter(ns.getString("output"));
					bw = new BufferedWriter(fw);
					if(client.get(bw, from_utc, to_utc)) {
						logger.info("get succeeded at this node: " + client.receiver);
					} else {
						boolean isDone = false;
						for(String node:nodes) {
							client = new JavaClient(node,port,host);
							if(client.get(bw, from_utc, to_utc)) {
								logger.info("get succeeded at this node: " + client.receiver);
								isDone = true;
								break;
							}
						}
						if(!isDone) {
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
			} else if(ns.getBoolean("upload")) {
				String fp = ns.getString("file");
				if(client.put(fp)) {
					logger.info("put succeeded at this node: " + client.receiver);
				} else {
					boolean isDone = false;
					for(String node:nodes) {
						client = new JavaClient(node,port,host);
						if(client.put(fp)) {
							logger.info("put succeeded at this node: " + client.receiver);
							isDone = true;
							break;
						}
					}
					if(!isDone) {
						logger.info("put failed at all other nodes");
					}
				}
			} else if(ns.getBoolean("ping")) {
				client.ping(ns.getString("message"));
			}
		} finally {
			client.shutdown();
		}

	}

}
