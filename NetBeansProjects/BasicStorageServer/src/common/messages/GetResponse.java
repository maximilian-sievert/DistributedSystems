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
public class GetResponse implements KVMessage {

    private final String key;
    private final String value;
    private final StatusType status;

    public GetResponse(String key, String value, StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }

    @Override
    public String getKey() {
        return this.key;
    }

    @Override
    public String getValue() {
        return this.value;
    }

    @Override
    public StatusType getStatus() {
        return this.status;
    }

}
