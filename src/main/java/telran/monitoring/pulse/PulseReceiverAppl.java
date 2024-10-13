package telran.monitoring.pulse;
import java.net.*;
import java.util.Arrays;

public class PulseReceiverAppl {
	private static final int PORT = 5000;
	private static final int MAX_BUFFER_SIZE = 1500;
	static DatagramSocket socket;
	public static void main(String[] args) throws Exception {
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
		System.out.println(json);
	}

}
