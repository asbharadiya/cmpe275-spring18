
package com.cmpe275.grpcComm;

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

    
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;



public class JavaClient {

	final static String CONST_MEDIA_TYPE_TEXT_MESOWEST = "mesowest";

	final static String CONST_MEDIA_TYPE_TEXT_MESONET = "mesonet";

	final static int CONST_MEDIA_TYPE_TEXT = 1;

	final static int CONST_CHUNK_SIZE = 2;

	final CountDownLatch done = new CountDownLatch(1);

	final static String CONST_MESOWEST_HEADER = "STN YYMMDD/HHMM MNET SLAT SLON SELV TMPF SKNT DRCT GUST PMSL ALTI DWPF RELH WTHR P24I";
	final static String CONST_MESONET_HEADER = "# id,name,mesonet,lat,lon,elevation,agl,cit,state,country,active";
	private final static String CONST_NEWLINE_CHAR = "\n";

	private static final Logger logger = Logger.getLogger(JavaClient.class.getName());

	private final static char CONST_DELIMITER = ',';

	private final static String[] CONST_STD_COL_LIST = CONST_MESOWEST_HEADER.replaceAll("\\s+", " ").split(" ");

	final static String MY_IP = "localhost";

	private final ManagedChannel channel;
	private final CommunicationServiceGrpc.CommunicationServiceStub stub;
	private final CommunicationServiceGrpc.CommunicationServiceBlockingStub blockingStub;

	public String sender = "";
	public String receiver = "";

	public JavaClient(String host, int port, String sender) {
		this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).build();
		this.stub = CommunicationServiceGrpc.newStub(channel);
		this.blockingStub = CommunicationServiceGrpc.newBlockingStub(channel);
		this.sender = sender;
		this.receiver = host;
		logger.info("Start client...");
		logger.info(this.sender);
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
			boolean is_mesonet = false;
			int current_chunk_size = 0;

			String[] names = fpath.split("/");
			String fileName = names[names.length - 1];
			String suspectedTimestamp;
			if (fileName.contains(".")){
				//System.out.println(fileName);
				String[] fileNameArray = fileName.split("\\.");
				suspectedTimestamp = fileNameArray[0];
			} else {
				suspectedTimestamp = fileName;
			}
			//System.out.println(suspectedTimestamp);
			String timestamp = "";
			if (suspectedTimestamp.matches("[0-9]{8}_[0-9]{4}")) {
				is_mesonet = true;
				timestamp = suspectedTimestamp;
			}

			String dataSourcePattern = "";
			if (!is_mesonet) {
				dataSourcePattern = CONST_MEDIA_TYPE_TEXT_MESOWEST;
			} else {
				dataSourcePattern = CONST_MEDIA_TYPE_TEXT_MESONET;
			}

			String timestampUTC = formatTimestampForMesonet(timestamp);

			File f = new File(fpath);
			FileReader fr = new FileReader(f);
			BufferedReader br = new BufferedReader(fr);
			StringBuffer sb = new StringBuffer();
			String line;
			//logger.info("Buffers UP!!!");
			while ((line = br.readLine()) != null) {
				logger.info("line:" + line);

				if (String.join(" ", line.trim().split("\\s+")).equalsIgnoreCase(CONST_MESOWEST_HEADER) || line.trim().equalsIgnoreCase(CONST_MESONET_HEADER)) {
					is_starts_reading = true;
					logger.info("Start reading...");
					continue;
				}
				if (!is_starts_reading) {
					continue;
				}

				String input = "";
				try{
					input = normalize(line, dataSourcePattern, timestampUTC);
				} catch (Exception e) {
					System.out.println("Unsuccessful normalization!");
					continue;
				}
				
				sb.append(input + "\n");
				current_chunk_size++;

				if (current_chunk_size == CONST_CHUNK_SIZE) {
					DatFragment datFragment = DatFragment.newBuilder().setData(ByteString.copyFromUtf8(sb.toString()))
							.build();
					logger.info("Data: " + sb.toString());
					Request req = Request.newBuilder()
							.setPutRequest(PutRequest.newBuilder().setDatFragment(datFragment).build()).build();
					requestObserver.onNext(req);

					current_chunk_size = 0;
					sb = new StringBuffer();
				}
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

	private String normalize(String line, String dataSourcePattern, String timestampUtc) {
		if (dataSourcePattern == CONST_MEDIA_TYPE_TEXT_MESONET) {
			return normalizeMesonetHelper(line, timestampUtc);
		} else if (dataSourcePattern == CONST_MEDIA_TYPE_TEXT_MESOWEST) {
			return normalizeMesowestHelper(line);
		} else {
			System.out.println("Unsupported data format.");
			return null;
		}
	}

	private String normalizeMesonetHelper(String line, String timestampUtc) {
		String[] cols = line.trim().replaceAll("\\s+", "").split(",");
		if (cols.length != 11) {
			System.out.println("Wrong number of colums!");
		}
		String res = cols[0] + CONST_DELIMITER + timestampUtc + CONST_DELIMITER + "NULL";

		for (int i = 3; i < 6; i++) {
			res += CONST_DELIMITER + cols[i];
		}

		for (int i = 6; i < CONST_STD_COL_LIST.length; i++) {
			res += CONST_DELIMITER + "NULL";
		}
		return res;

	}

	private String normalizeMesowestHelper(String line) {

		String[] cols = line.trim().replaceAll("\\s+", " ").split(" ");
		String timestampUtc = formatTimestampForMesowest(cols[1]);
		String res = cols[0] + CONST_DELIMITER + timestampUtc;
		for (int i = 2; i < cols.length; i++) {
			res += CONST_DELIMITER + cols[i];
		}
		return res;
	}

	private String formatTimestampForMesowest(String timestamp) {
		String[] terms = timestamp.trim().split("/");

		if (terms.length != 2) {
			System.out.println("Wrong format for Mesowest timestamp");
			return null;
		}

		String year = terms[0].substring(0, 4);
		String month = terms[0].substring(4, 6);
		String day = terms[0].substring(6, 8);
		String hr = terms[1].substring(0, 2);
		String min = terms[1].substring(2, 4);

		String res = year + "-" + month + "-" + day + " " + hr + ":" + min + ":00";
		return res;
	}

	private String formatTimestampForMesonet(String timestamp) {
		String[] terms = timestamp.trim().split("_");

		if (terms.length != 2) {
			System.out.println("Wrong format for Mesowest timestamp");
			return null;
		}

		String year = terms[0].substring(0, 4);
		String month = terms[0].substring(4, 6);
		String day = terms[0].substring(6, 8);
		String hr = terms[1].substring(0, 2);
		String min = terms[1].substring(2, 4);

		String res = year + "-" + month + "-" + day + " " + hr + ":" + min + ":00";
		return res;
	}

	public boolean get(BufferedWriter fp, String from_utc, String to_utc, String param_json) {
		// if (param_json != null) {
		// 	dosomething
		// }

		String uuid = UUID.randomUUID().toString();
		QueryParams queryParams = QueryParams.newBuilder().setFromUtc(from_utc).setToUtc(to_utc).setParamJson(param_json).build();
		MetaData metaData = MetaData.newBuilder().setUuid(uuid).build();
		GetRequest getRequest = GetRequest.newBuilder().setMetaData(metaData).setQueryParams(queryParams).build();
		Request req = Request.newBuilder().setFromSender(this.sender).setToReceiver(this.receiver).setGetRequest(getRequest).build();

		Iterator<Response> it;
		try {
			fp.write(CONST_MESOWEST_HEADER + CONST_NEWLINE_CHAR);
		} catch (IOException e) {
			System.out.println("Cannot write!");
			return false;
		}
		
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
						fp.write(str + CONST_NEWLINE_CHAR);
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
}
