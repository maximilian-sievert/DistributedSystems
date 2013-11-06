/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package common.messages;

/**
 *
 * @author Maximilian
 */
public class PutResponse implements KVMessage {

    private final String key;
    private final StatusType status;

    public PutResponse(String key, StatusType status) {
        this.key = key;
        this.status = status;
    }

    @Override
    public String getKey() {
        return this.key;
    }

    @Override
    public String getValue() {
        return null;
    }

    @Override
    public StatusType getStatus() {
        return this.status;
    }
}
