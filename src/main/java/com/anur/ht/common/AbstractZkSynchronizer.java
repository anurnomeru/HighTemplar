package com.anur.ht.common;

import java.util.List;
import java.util.Map;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;

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
        this.zkClient = zkClient;
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

    abstract protected String tryAcquire(Integer generatedNode, Map<String, List<Integer>> childNodes);

    protected Integer genNode(String specialSign) {

        Integer node = null;
        try {
            node = nodeTranslation(zkClient.createEphemeralSequential(nodePath + genNodeName(specialSign), null), nodePath);
        } catch (ZkNoNodeException e) {
            // 首次创建节点由于没有上层节点可能会报错
            zkClient.createPersistent(nodePath, true);
            genNode(specialSign);
        }
        return node;
    }

    protected Map<String, List<Integer>> getChildren() {
        return nodeTranslation(zkClient.getChildren(nodePath));
    }

    public static class Mutex extends AbstractZkSynchronizer {

        public Mutex(String lockName, ZkClient zkClient) {
            super(lockName, zkClient);
        }

        @Override
        protected String tryAcquire(Integer generatedNode, Map<String, List<Integer>> childNodes) {
            System.out.println(generatedNode);
            System.out.println(childNodes);
            return null;
        }
    }

    public static void main(String[] args) {
        Mutex mutex = new Mutex("MY-LORD", new ZkClient("127.0.0.1"));
        mutex.acquire("REA-LK");
        mutex.acquire("WRI-LK");
        mutex.acquire("WRI-LK");
        mutex.acquire("WRI-LK");
        mutex.acquire("WRI-LK");
        mutex.acquire("REA-LK");
        mutex.acquire("REA-LK");
        mutex.acquire("REA-LK");

    }
}
