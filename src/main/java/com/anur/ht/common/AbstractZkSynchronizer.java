package com.anur.ht.common;

import java.util.List;
import java.util.Optional;
import org.I0Itec.zkclient.ZkClient;
import com.sun.org.apache.xerces.internal.dom.ChildNode;

/**
 * Created by Anur IjuoKaruKas on 2019/6/16
 */
public abstract class AbstractZkSynchronizer {

    /**
     * Avoid using this path for other purposes when using zookeeper
     *
     * 使用 zookeeper 时避免此路径作他用
     */
    private static String PREFIX = "/HIGH-TEMPLAR";

    private static String SUFFIX = "/";

    /**
     * we use this path to acquire ephemeral sequential
     */
    private String nodePath;

    private ZkClient zkClient;

    /**
     * Please make sure that different lock has uniquely lockName, we
     * define same lock in cluster with same lockName.
     *
     * 确保不同的锁使用不同的 lockName，同一个锁则依靠同一锁名来进行同步控制
     */
    public AbstractZkSynchronizer(String lockName, ZkClient zkClient) {
        if (!lockName.endsWith(SUFFIX)) {
            lockName += SUFFIX;
        }
        this.nodePath = PREFIX + lockName;
    }

    protected void acquire(String specialSign) {
        String theNodeToWaitSignal;
        if ((theNodeToWaitSignal = tryAcquire(genNode(specialSign), getChildren())) == null) {
            return;
        }


    }

    abstract protected String tryAcquire(String generatedNode, List<String> childNode);

    protected String genNode(String specialSign) {
        return zkClient.createEphemeralSequential(specialSign == null ? nodePath : nodePath + specialSign, null);
    }

    protected List<String> getChildren() {
        return zkClient.getChildren(nodePath);
    }
}
