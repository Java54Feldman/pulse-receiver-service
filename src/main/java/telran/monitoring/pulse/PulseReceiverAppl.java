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
	private static final String MAX_THRESHOLD_PULSE_VALUE = "MAX_THRESHOLD_PULSE_VALUE";
	private static final String MIN_THRESHOLD_PULSE_VALUE = "MIN_THRESHOLD_PULSE_VALUE";
	private static final String WARN_MAX_PULSE_VALUE = "WARN_MAX_PULSE_VALUE";
	private static final String WARN_MIN_PULSE_VALUE = "WARN_MIN_PULSE_VALUE";
	private static final String PATIENT_ID_FIELD = "patientId";
	private static final String TIMESTAMP_FIELD = "timestamp";
	private static final String VALUE_FIELD = "value";
	static String envLogLevel = System.getenv(LOGGING_LEVEL).toUpperCase();
	static int envMaxThresholdPulse = Integer.valueOf(System.getenv(MAX_THRESHOLD_PULSE_VALUE));
	static int envMinThresholdPulse = Integer.valueOf(System.getenv(MIN_THRESHOLD_PULSE_VALUE));
	static int envWarnMaxPulse = Integer.valueOf(System.getenv(WARN_MAX_PULSE_VALUE));
	static int envWarnMinPulse = Integer.valueOf(System.getenv(WARN_MIN_PULSE_VALUE));
	static Logger logger = Logger.getLogger(PulseReceiverAppl.class.getName());
	static DatagramSocket socket;
	static DynamoDbClient client; 
	static {
		System.setProperty("java.util.logging.SimpleFormatter.format", 
                "[%1$tF %1$tT.%1$tL] [%2$s] [%4$s]: %5$s%6$s%n");
	}

	public static void main(String[] args) throws Exception {
		logger.setUseParentHandlers(false); //removing duplicate logs
		Level logLevel = Level.parse(envLogLevel);
		logger.setLevel(logLevel);
		Handler handler = new ConsoleHandler();
		handler.setLevel(logLevel);
		logger.addHandler(handler);
	    logger.config("Environment variable " + LOGGING_LEVEL + "=" + envLogLevel);
	    logger.config("Environment variable " + MAX_THRESHOLD_PULSE_VALUE + "=" + envMaxThresholdPulse);
	    logger.config("Environment variable " + MIN_THRESHOLD_PULSE_VALUE + "=" + envMinThresholdPulse);
	    logger.config("Environment variable " + WARN_MAX_PULSE_VALUE + "=" + envWarnMaxPulse);
	    logger.config("Environment variable " + WARN_MIN_PULSE_VALUE + "=" + envWarnMinPulse);
	    
	    try {
			client = DynamoDbClient.builder()
				.region(Region.US_EAST_1)
				.credentialsProvider(StaticCredentialsProvider.create(
						AwsBasicCredentials.create(
								System.getenv(AWS_ACCESS_KEY_ID),
								System.getenv(AWS_SECRET_KEY)
								)))
				.build();
		        client.describeTable(DescribeTableRequest.builder().tableName(TABLE_NAME).build());
		        logger.info("Connected to database: DynamoDB AWS, table=" + TABLE_NAME);
	    } catch (DynamoDbException e) {
	    	logger.severe("Failed to connect to DynamoDB: " + e.getMessage());
	    }
		
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
		checkPulseValue(json, request);
		
	}

	private static void checkPulseValue(String json, PutItemRequest request) {
		int pulseValue = Integer.valueOf(request.item()
				.get(VALUE_FIELD)
				.getValueForField("N", String.class)
				.get());
		if(pulseValue > envWarnMaxPulse && pulseValue <= envMaxThresholdPulse) {
			logger.warning("The pulse is too high " + json);
		} else if(pulseValue < envWarnMinPulse && pulseValue >= envMinThresholdPulse) {
			logger.warning("The pulse is too low " + json);
		} else if(pulseValue < envMinThresholdPulse && pulseValue > envMaxThresholdPulse) {
			logger.severe("The pulse out of range" + json);
		}
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
