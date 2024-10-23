package telran.monitoring.pulse;

import java.net.*;
import java.util.*;
import java.util.stream.Collectors;

import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.logging.*;

public class PulseReceiverAppl {
	private static final int PORT = 5000;
	private static final int MAX_BUFFER_SIZE = 1500;
	private static final String AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID";
	private static final String AWS_SECRET_KEY = "AWS_SECRET_KEY";
	private static final String TABLE_NAME = "pulse_values";
	private static final String LOGGING_LEVEL = "LOGGING_LEVEL";
	private static final String PATIENT_ID_FIELD = "patientId";
	private static final String TIMESTAMP_FIELD = "timestamp";
	static DatagramSocket socket;
	static DynamoDbClient client = DynamoDbClient.builder()
			.region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        System.getenv(AWS_ACCESS_KEY_ID),
                        System.getenv(AWS_SECRET_KEY)
                    )))
			.build();
	static Logger logger = Logger.getLogger(PulseReceiverAppl.class.getName());
	static {
		System.setProperty("java.util.logging.SimpleFormatter.format", 
                "[%1$tF %1$tT.%1$tL] [%2$s] [%4$s]: %5$s%6$s%n");
	}

	public static void main(String[] args) throws Exception {
		logger.setUseParentHandlers(false); //removing duplicate logs
		Level logLevel = Level.parse(System.getenv(LOGGING_LEVEL).toUpperCase());
		Handler handler = new ConsoleHandler();
		handler.setLevel(logLevel);
	    logger.addHandler(handler);
	    logger.setLevel(logLevel);
		
		socket = new DatagramSocket(PORT);
		byte[] buffer = new byte[MAX_BUFFER_SIZE];
		while (true) {
			DatagramPacket packet = new DatagramPacket(buffer, MAX_BUFFER_SIZE);
			socket.receive(packet);
			processReceivedData(buffer, packet);
		}
	}

	private static void processReceivedData(byte[] buffer, DatagramPacket packet) {
		String json = new String(Arrays.copyOf(buffer, packet.getLength()));
		logger.fine("Received pulse data " + json);

		Map<String, AttributeValue> item = Arrays.stream(json.replaceAll("[{}\"\\s]", "").split(","))
				.map(pair -> pair.split(":")).filter(keyValue -> keyValue.length == 2)
				.collect(Collectors.toMap(
						keyValue -> keyValue[0], 
						keyValue -> AttributeValue.builder().n(keyValue[1]).build()));

		PutItemRequest request = PutItemRequest.builder()
				.tableName(TABLE_NAME)
				.item(item)
				.build();

		client.putItem(request);
		logSavedPulseData(request);	
	}

	private static void logSavedPulseData(PutItemRequest request) {
		String patientId = request.item()
				.get(PATIENT_ID_FIELD)
				.getValueForField("N", String.class)
				.get();
		String timestamp = request.item()
				.get(TIMESTAMP_FIELD)
				.getValueForField("N", String.class)
				.get();
		logger.finer(String.format("Saved pulse data to DynamoDB: "
				+ PATIENT_ID_FIELD + "=%s, " + TIMESTAMP_FIELD + "=%s", patientId, timestamp));
	}

}
