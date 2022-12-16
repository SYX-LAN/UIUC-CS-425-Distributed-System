import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;

import static utils.Constant.INDICATOR;
import static utils.Constant.NEW_MASTER;

public class MasterIndicator {
    public static void main(String[] args) {
        while (true) {
            try {
                byte[] buffer = new byte[1024];
                DatagramSocket socket = new DatagramSocket(INDICATOR);
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                String receive = new String(packet.getData(), packet.getOffset(),
                        packet.getLength(), StandardCharsets.UTF_8);
                JSONObject msg = new JSONObject(receive);
                String newMaster = (String) msg.get(NEW_MASTER);
                File file = new File(new File("").getAbsolutePath() + "/utils/master.txt");
                FileWriter fileWriter = new FileWriter(file);
                fileWriter.write("");
                fileWriter.write(newMaster);
                fileWriter.flush();
                fileWriter.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
