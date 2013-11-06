package client;

import DistributedSystems.DSConnection;
import common.messages.KVMessage;
import common.messages.PutResponse;
import common.messages.GetResponse;
import common.messages.KVMessage.StatusType;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KVStore implements KVCommInterface {

    private static DSConnection connection;
    private final String address;
    private final int port;

    public static String ReadString(byte[] message, int start) {
        String r = "";
        while (message[start] != 0 && start < message.length) {
            r += (char) message[start];
            start++;
        }
        return r;
    }

    /**
     * Initialize KVStore with address and port of KVServer
     *
     * @param address the address of the KVServer
     * @param port the port of the KVServer
     */
    public KVStore(String address, int port) {
        this.address = address;
        this.port = port;
    }

    @Override
    public void connect() throws Exception {
        connection.connect(address, port, 5000);
    }

    @Override
    public void disconnect() {
        try {
            connection.disconnect();
        } catch (IOException ex) {
            Logger.getLogger(KVStore.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public KVMessage put(String key, String value) throws Exception {
        //build message: opcode, key \0 value \0 \n
        byte[] b_key = key.getBytes();
        byte[] b_value = value.getBytes();
        byte[] message = new byte[b_key.length + b_value.length + 2];
        message[0] = 1;
        System.arraycopy(b_key, 0, message, 1, b_key.length);
        System.arraycopy(b_value, 0, message, 2 + b_key.length, b_value.length);
        //send message
        connection.send(message);

        //Wait for response...
        byte[] r_data = connection.receive();

        StatusType r_status = (KVMessage.StatusType.values())[r_data[0]];
        String r_key = ReadString(r_data, 1);
        return new PutResponse(r_key, r_status);
    }

    @Override
    public KVMessage get(String key) throws Exception {
        //build message: opcode, key \0 value \0 \n
        byte[] b_key = key.getBytes();
        byte[] message = new byte[b_key.length + 1];
        message[0] = 2;
        System.arraycopy(b_key, 0, message, 1, b_key.length);

        //send message
        connection.send(message);

        //Wait for response...
        byte[] r_data = connection.receive();
        StatusType r_status = (KVMessage.StatusType.values())[r_data[0]];
        String r_key = ReadString(r_data, 1);
        String r_value = ReadString(r_data, 1 + r_key.length() + 1); //Skip StatusByte, Key and zero terminator
        return new GetResponse(r_key, r_value, r_status);
    }

}
