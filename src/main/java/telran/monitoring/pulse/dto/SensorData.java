package telran.monitoring.pulse.dto;

import org.json.JSONObject;

public record SensorData(long seqNumber, long patientId, int value, long timestamp) {
	public static SensorData getSensorData(String json) {
		JSONObject jsonObj = new JSONObject(json);
		return new SensorData(jsonObj.getLong("seqNumber"), 
				jsonObj.getLong("patientId"), 
				jsonObj.getInt("value"),
				jsonObj.getLong("timestamp"));
	}

	@Override
	public String toString() {
		JSONObject jsonObj = new JSONObject();
		jsonObj.put("seqNumber", seqNumber);
		jsonObj.put("patientId", patientId);
		jsonObj.put("value", value);
		jsonObj.put("timestamp", timestamp);
		return jsonObj.toString();
	}

}
