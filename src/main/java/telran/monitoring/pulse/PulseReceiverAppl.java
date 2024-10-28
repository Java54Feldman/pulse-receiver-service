package telran.monitoring.pulse;

import java.net.*;
import java.util.*;

import telran.monitoring.pulse.dto.SensorData;

import static telran.monitoring.pulse.PulseReceiverDefaultConfigValues.*;
import java.util.logging.*;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest.Builder;

public class PulseReceiverAppl {
	static PulseReceiverConfigValues configValues;
	static DatagramSocket socket;
	static DynamoDbClient client = DynamoDbClient.builder().build();
	static Builder request;

	static Logger logger = Logger.getLogger("PulseReceiverAppl");

	public static void main(String[] args) throws Exception {
		configValues = PulseReceiverConfigValues.getConfigValues(logger);
		request = PutItemRequest.builder().tableName(configValues.getTableName());

		setLogger();

		logger.config(configValues.toString());
		logger.info("DynamoDB table is " + configValues.getTableName());
		socket = new DatagramSocket(DEFAULT_PORT);
		byte[] buffer = new byte[DEFAULT_MAX_BUFFER_SIZE];
		while (true) {
			DatagramPacket packet = new DatagramPacket(buffer, DEFAULT_MAX_BUFFER_SIZE);
			socket.receive(packet);
			processReceivedData(packet);
		}

	}

	private static void setLogger() {
		LogManager.getLogManager().reset();
		Handler handler = new ConsoleHandler();
		logger.setLevel(configValues.getLoggerLevel());
		handler.setLevel(Level.FINEST);
		logger.addHandler(handler);

	}

	private static void processReceivedData(DatagramPacket packet) {
		String json = new String(Arrays.copyOf(packet.getData(), packet.getLength()));
		logger.fine(json);
		SensorData sensorData = SensorData.getSensorData(json);
		client.putItem(request.item(getMapItem(sensorData)).build());

		logger.finer(String.format("table: %s added item with partition key is %d," + " sorted key is %d",
				configValues.getTableName(), sensorData.patientId(), sensorData.timestamp()));
		logAbnormalValue(sensorData);

	}

	private static Map<String, AttributeValue> getMapItem(SensorData sensorData) {
		HashMap<String, AttributeValue> res = new HashMap<>();
		res.put("patientId", AttributeValue.builder().n(sensorData.patientId() + "").build());
		res.put("timestamp", AttributeValue.builder().n(sensorData.timestamp() + "").build());
		res.put("value", AttributeValue.builder().n(sensorData.value() + "").build());
		return res;
	}

	private static void logAbnormalValue(SensorData sensorData) {
		int pulseValue = sensorData.value();
		if (pulseValue > configValues.getMaxThresholdPulse()) {
			logger.severe(String.format("pulse value greater than %d, sensor data %s",
					configValues.getMaxThresholdPulse(), sensorData));
		} else if (pulseValue < configValues.getMinThresholdPulse()) {
			logger.severe(String.format("pulse value less than %d, sensor data %s", configValues.getMinThresholdPulse(),
					sensorData));
		} else if (pulseValue > configValues.getWarnMaxPulse()) {
			logger.warning(String.format("pulse value greater than %d, sensor data %s", configValues.getWarnMaxPulse(),
					sensorData));
		} else if (pulseValue < configValues.getWarnMinPulse()) {
			logger.warning(String.format("pulse value less than %d, sensor data %s", configValues.getWarnMinPulse(),
					sensorData));
		}

	}

}