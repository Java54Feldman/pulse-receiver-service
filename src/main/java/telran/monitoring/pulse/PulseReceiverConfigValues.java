package telran.monitoring.pulse;

import static telran.monitoring.pulse.PulseReceiverDefaultConfigValues.*;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PulseReceiverConfigValues {
	private Level loggerLevel;
	static final String LOGGING_LEVEL = "LOGGING_LEVEL";
	static Logger logger;
	static final String TABLE_NAME = "TABLE_NAME";
	static final String MAX_THRESHOLD_PULSE_VALUE = "MAX_THRESHOLD_PULSE_VALUE";
	static final String MIN_THRESHOLD_PULSE_VALUE = "MIN_THRESHOLD_PULSE_VALUE";
	static final String WARN_MAX_PULSE_VALUE = "WARN_MAX_PULSE_VALUE";
	static final String WARN_MIN_PULSE_VALUE = "WARN_MIN_PULSE_VALUE";
	static PulseReceiverConfigValues configValues = null;
	private int maxThresholdPulse;
	private int minThresholdPulse;
	private int warnMaxPulse;
	private int warnMinPulse;
	private String tableName;

	private PulseReceiverConfigValues(Level loggerLevel, int maxThresholdPulse, int minThresholdPulse, int warnMaxPulse,
			int warnMinPulse, String tableName) {
		this.loggerLevel = loggerLevel;
		this.maxThresholdPulse = maxThresholdPulse;
		this.minThresholdPulse = minThresholdPulse;
		this.warnMaxPulse = warnMaxPulse;
		this.warnMinPulse = warnMinPulse;
		this.tableName = tableName;
	}

	synchronized public static PulseReceiverConfigValues getConfigValues(Logger logger) {
		if (configValues == null) {
			PulseReceiverConfigValues.logger = logger;
			String tableName = getTableNameEnv();
			Level loggerLevel = getLoggerValue();
			int maxThresholdPulse = getPulseValue(MAX_THRESHOLD_PULSE_VALUE, DEFAULT_MAX_THRESHOLD_PULSE_VALUE);
			int minThresholdPulse = getPulseValue(MIN_THRESHOLD_PULSE_VALUE, DEFAULT_MIN_THRESHOLD_PULSE_VALUE);
			int warnMaxPulse = getPulseValue(WARN_MAX_PULSE_VALUE, DEFAULT_WARN_MAX_PULSE_VALUE);
			int warnMinPulse = getPulseValue(WARN_MIN_PULSE_VALUE, DEFAULT_WARN_MIN_PULSE_VALUE);
			configValues = new PulseReceiverConfigValues(loggerLevel, maxThresholdPulse, minThresholdPulse,
					warnMaxPulse, warnMinPulse, tableName);
		}
		return configValues;
	}

	private static String getTableNameEnv() {
		String res = DEFAULT_TABLE_NAME;
		String envTableName = System.getenv(TABLE_NAME);
		if (envTableName != null) {
			res = envTableName;
		}
		return res;
	}

	private static Level getLoggerValue() {
		String loggerLevelStr = "";
		Level level = Level.parse(DEFAULT_LOGGING_LEVEL);

		try {
			loggerLevelStr = System.getenv(LOGGING_LEVEL);
			if (loggerLevelStr != null) {
				level = Level.parse(loggerLevelStr.toUpperCase());
			}
		} catch (Exception e) {
			logger.severe(loggerLevelStr + " Wrong Logging Level");
		}
		return level;
	}

	private static int getPulseValue(String envName, int defaultValue) {
		int res = defaultValue;
		String valueStr = "";
		try {
			valueStr = System.getenv(envName);
			if (valueStr != null) {
				res = Integer.parseInt(valueStr);
			}

		} catch (Exception e) {
			logger.severe(String.format("%s - wrong value of %s", valueStr, envName));
		}
		return res;
	}

	@Override
	public String toString() {
		return "PulseReceiverConfigValues [loggerLevel=" + loggerLevel + ", maxThresholdPulse=" + maxThresholdPulse
				+ ", minThresholdPulse=" + minThresholdPulse + ", warnMaxPulse=" + warnMaxPulse + ", warnMinPulse="
				+ warnMinPulse + "]";
	}

	public Level getLoggerLevel() {
		return loggerLevel;
	}

	public static Logger getLogger() {
		return logger;
	}

	public static PulseReceiverConfigValues getConfigValues() {
		return configValues;
	}

	public int getMaxThresholdPulse() {
		return maxThresholdPulse;
	}

	public int getMinThresholdPulse() {
		return minThresholdPulse;
	}

	public int getWarnMaxPulse() {
		return warnMaxPulse;
	}

	public int getWarnMinPulse() {
		return warnMinPulse;
	}

	public String getTableName() {
		return tableName;
	}

}