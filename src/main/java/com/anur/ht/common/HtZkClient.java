package com.anur.ht.common;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.I0Itec.zkclient.serialize.ZkSerializer;

/**
 * Created by Anur IjuoKaruKas on 2019/6/22
 */
public class HtZkClient {

    private final String zkServers;

    private int sessionTimeout = 30000;

    private int connectionTimeout = 30000;

    private ZkSerializer zkSerializer = new SerializableSerializer();

    private long operationRetryTimeout = -1;

    public HtZkClient(String zkServers) {
        this.zkServers = zkServers;
    }

    public HtZkClient(String zkServers, int sessionTimeout, int connectionTimeout, ZkSerializer zkSerializer, long operationRetryTimeout) {
        this.zkServers = zkServers;
        this.sessionTimeout = sessionTimeout;
        this.connectionTimeout = connectionTimeout;
        this.zkSerializer = zkSerializer;
        this.operationRetryTimeout = operationRetryTimeout;
    }

    public ZkClient gen() {
        return new ZkClient(zkServers, sessionTimeout, connectionTimeout, zkSerializer, operationRetryTimeout);
    }
}
