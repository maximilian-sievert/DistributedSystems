package client;

import DistributedSystems.DSConnection;
import common.messages.*;
import common.messages.KVMessage.StatusType;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KVStore implements KVCommInterface {

    private final DSConnection connection;
    private final String address;
    private final int port;

    /**
     * Initialize KVStore with address and port of KVServer
     *
     * @param address the address of the KVServer
     * @param port the port of the KVServer
     */
    public KVStore(String address, int port) {
        this.connection = new DSConnection();
        this.address = address;
        this.port = port;
    }

    @Override
    public void connect() throws Exception {
        connection.connect(address, port, 25000);
    }

    @Override
    public void disconnect() {
        try {
            connection.disconnect();
        } catch (IOException ex) {
            Logger.getLogger(KVStore.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private KVMessage sendreceive(StatusType status, String key, String value) throws IOException {
        KVBasicMessage m = new KVBasicMessage(key, value, status);
        Logger.getLogger(KVStore.class.getName()).log(Level.INFO, "KVStore >> {0}", m.toString());
        byte[] data = m.GetData();
        connection.send(data);

        //Logger.getLogger(KVStore.class.getName()).log(Level.INFO, "KVStore waiting for message...");
        KVBasicMessage r = new KVBasicMessage(connection.receive());
        Logger.getLogger(KVStore.class.getName()).log(Level.INFO, "KVStore << {0}", r.toString());
        return r;
    }

    @Override
    public KVMessage put(String key, String value) throws IOException {
        return sendreceive(StatusType.PUT, key, value);
    }

    @Override
    public KVMessage get(String key) throws IOException {
        return sendreceive(StatusType.GET, key, null);
    }
}
