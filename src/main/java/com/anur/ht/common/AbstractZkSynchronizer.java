package com.anur.ht.common;

import java.util.List;
import org.I0Itec.zkclient.ZkClient;

/**
 * Created by Anur IjuoKaruKas on 2019/6/16
 */
public abstract class AbstractZkSynchronizer extends NodeOperator {

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
        this.nodePath = genNodePath(lockName);
    }

    /**
     * specialSign 的长度我们规定为 6，这是为了获取 children 时方便截取
     */
    protected void acquire(String specialSign) {
        String theNodeToWaitSignal;
        if ((theNodeToWaitSignal = tryAcquire(genNode(specialSign), getChildren())) == null) {
            return;
        }
    }

    abstract protected String tryAcquire(Integer generatedNode, List<Integer> childNode);

    protected Integer genNode(String specialSign) {
        return nodeTranslation(zkClient.createEphemeralSequential(nodePath + genNodeName(specialSign), null));
    }

    protected List<Integer> getChildren() {
        return nodeTranslation(zkClient.getChildren(nodePath));
    }
}
