package app_kvServer;

import client.KVStore;
import common.messages.KVMessage;
import common.messages.KVBasicMessage;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KVServer {

    private final int num_threads = 10;
    private final Thread[] threads = new Thread[num_threads];
    private final ConcurrentHashMap<String, String> local_hashmap = new ConcurrentHashMap<String, String>();

    private boolean shutdown_requested = false;

    public boolean isShutdownRequested() {
        return this.shutdown_requested;
    }

    public void RequestShutdown() {
        this.shutdown_requested = true;
    }
    private final int port;

    public int getPort() {
        return this.port;
    }
    private final ServerSocket serverSocket;

    public ServerSocket getServerSocket() {
        return this.serverSocket;
    }

    /**
     * Start KV Server at given port
     *
     * @param port given port for storage server to operate
     * @throws java.io.IOException
     */
    public KVServer(int port) throws IOException {
        this.port = port;
        this.serverSocket = new ServerSocket(this.port);
    }

    public void runServer() {
        for (int i = 0; i < num_threads; i++) {
            (threads[i] = new Thread(new ServerThread(this))).start();
            threads[i].setName("ServerWorker " + i);
        }
        try {
            for (Thread t : threads) {
                t.join();
            }
        } catch (InterruptedException ex) {
            Logger.getLogger(KVServer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public synchronized KVBasicMessage put(String key, String value) {
        try {
            if (this.local_hashmap.containsKey(key)) {
                this.local_hashmap.put(key, value);
                return new KVBasicMessage(key, value, KVMessage.StatusType.PUT_UPDATE);
            } else {
                this.local_hashmap.put(key, value);
                return new KVBasicMessage(key, value, KVMessage.StatusType.PUT_SUCCESS);
            }
        } catch (Exception e) {
            return new KVBasicMessage(key, value, KVMessage.StatusType.PUT_ERROR);
        }

    }

    public synchronized KVBasicMessage get(String key) {
        if (this.local_hashmap.containsKey(key)) {
            String value = this.local_hashmap.get(key);
            return new KVBasicMessage(key, value, KVMessage.StatusType.GET_SUCCESS);
        }
        return new KVBasicMessage(key, null, KVMessage.StatusType.GET_ERROR);
    }

    public static void main(String[] args) throws IOException {
        final KVServer sv = new KVServer(55555);
        Logger.getLogger(ServerThread.class.getName()).log(Level.INFO, "Server initialized.");
        (new Thread() {
            @Override
            public void run() {
                Logger.getLogger(ServerThread.class.getName()).log(Level.INFO, "Server starting in its thread...");
                sv.runServer();
            }
        }).start();
        try {
            KVStore mystore = new KVStore("localhost", 55555);
            Logger.getLogger(ServerThread.class.getName()).log(Level.INFO, "KVStore initialized.");
            for (int i = 0; i < 10; i++) {
                mystore.connect();
                Logger.getLogger(ServerThread.class.getName()).log(Level.INFO, "KVStore connected.");
                for (int k = 0; k < 10; k++) {
                    mystore.put("k" + k, "v" + k + "_"+i);
                    mystore.put("k" + k, "v" + k + "-"+i);
                    //maybe sleep, run multithreaded clients
                    String kv = mystore.get("k" + k).getValue();
                    if(!kv.equals("v" + k + "-"+i)){ //should be 
                        Logger.getLogger(ServerThread.class.getName()).log(Level.SEVERE, "[ERROR] KVStore missmatch: {0}: {1}!={2}", new Object[]{k, kv,"v" + k + "-"+i});
                    }
                }
                mystore.disconnect();
                Logger.getLogger(ServerThread.class.getName()).log(Level.INFO, "KVStore disconnected.");
            }
        } catch (InterruptedException ex) {
            Logger.getLogger(KVServer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(KVServer.class.getName()).log(Level.SEVERE, null, ex);
        }
        sv.RequestShutdown();
    }
}
