package telran.monitoring.pulse;

public interface PulseReceiverDefaultConfigValues {
	int DEFAULT_PORT = 5000;
	int DEFAULT_MAX_BUFFER_SIZE = 1500;
	String DEFAULT_TABLE_NAME = "pulse_values";
	String DEFAULT_LOGGING_LEVEL = "INFO";
	int DEFAULT_MAX_THRESHOLD_PULSE_VALUE = 210;
	int DEFAULT_MIN_THRESHOLD_PULSE_VALUE = 40;
	int DEFAULT_WARN_MAX_PULSE_VALUE = 180;
	int DEFAULT_WARN_MIN_PULSE_VALUE = 55;

}