/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package app_kvServer;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVBasicMessage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Maximilian
 */
public class ServerThread implements Runnable {

    private final KVServer parentserver;

    public ServerThread(KVServer p) {
        this.parentserver = p;
    }

    public void handlesession(Socket clientSocket) throws IOException, SocketException{
        while(clientSocket.isConnected()){
            InputStream cl_in = clientSocket.getInputStream();

            ArrayList<Byte> messagebuffer = new ArrayList<Byte>();
            int val;
            while ((val = cl_in.read()) != -1) { //-1 : end of stream (possibly network issue?)
                if (val == 13) { //= '\n', Packet delimiter
                    break;
                } else {
                    messagebuffer.add((byte) val);
                }
            }
            if(val==-1){ //Socket connection has been lost, so we presumably cannot send an answer.
                throw new SocketException("Socket connection to client has been lost, ready to accept further connections...");
            }
            byte[] data = new byte[messagebuffer.size()];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) messagebuffer.get(i);
            }

            //handle packet
            KVBasicMessage request = new KVBasicMessage(data);
            Logger.getLogger(ServerThread.class.getName()).log(Level.INFO, "Serverthread << {0}", request.toString());
            KVBasicMessage response = processMessage(request);
            Logger.getLogger(ServerThread.class.getName()).log(Level.INFO, "Serverthread >> {0}", response.toString());
            //send response
            OutputStream cl_out = clientSocket.getOutputStream();
            cl_out.write(response.GetData());
            cl_out.write((byte) 13); //Packet delimiter
            cl_out.flush();
        }
    }
    @Override
    public void run() {
        //service loop
        while (!this.parentserver.isShutdownRequested()) {
            try {
                Socket clientSocket = this.parentserver.getServerSocket().accept();
                handlesession(clientSocket); //This loops until the socket is lost.
            } catch (SocketException soex) {
                Logger.getLogger(ServerThread.class.getName()).log(Level.INFO, "Socket has been disconnected.");
            } catch (IOException ioex) {
                Logger.getLogger(ServerThread.class.getName()).log(Level.SEVERE, null, ioex);
            }
        }
    }

    private KVBasicMessage processMessage(KVBasicMessage request) {
        if (request.getStatus() == StatusType.PUT) {
            return this.parentserver.put(request.getKey(), request.getValue());
        } else if (request.getStatus() == StatusType.GET) {
            return this.parentserver.get(request.getKey());
        } else {
            return new KVBasicMessage(null, null, KVMessage.StatusType.DELETE_ERROR);
        }
    }
}
