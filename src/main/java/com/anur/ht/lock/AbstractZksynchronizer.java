package com.anur.ht.lock;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import com.anur.ht.exception.HighTemplarException;

/**
 * Created by Anur IjuoKaruKas on 2019/6/16
 */
public abstract class AbstractZksynchronizer extends NodeOperator {

    /**
     * we use this path to acquire ephemeral sequential
     */
    private String nodePath;

    private ZkClient zkClient;

    private ConcurrentHashMap<Thread, NodeInfo> threadKeeper = new ConcurrentHashMap<>();

    /**
     * Please make sure that different lock has uniquely lockName, we
     * define same lock in cluster with same lockName.
     *
     * 确保不同的锁使用不同的 lockName，同一个锁则依靠同一锁名来进行同步控制
     */
    public AbstractZksynchronizer(String lockName, ZkClient zkClient) {
        this.nodePath = genNodePath(lockName);
        this.zkClient = zkClient;
    }

    /**
     * specialSign 的长度我们规定为 6，这是为了获取 children 时方便截取
     */
    void acquire(String nodeName) {
        Thread currentThread = Thread.currentThread();
        NodeInfo nodeInfo;

        // 如果是重入的话，nodeInfo.incr
        if (threadKeeper.containsKey(currentThread) && (nodeInfo = threadKeeper.get(currentThread)).isSuccessor()) {
            nodeInfo.incr();
            return;
        }

        // 从 zkClient 中生成一个当前节点的 node
        nodeInfo = genNodeFromZk(nodeName);
        threadKeeper.put(currentThread, nodeInfo);
        acquireQueue(nodeInfo.node);
    }

    private void acquireQueue(int node) {
        for (; ; ) {
            String theNodeToWaitSignal;

            // 由子类判断是否成功获取锁
            if ((theNodeToWaitSignal = tryAcquire(node, getChildren())) != null) {

                // 如果失败则进入等待
                CountDownLatch cdl = new CountDownLatch(1);
                final IZkDataListener zkDataListener = new IZkDataListener() {

                    @Override
                    public void handleDataChange(String s, Object o) {

                    }

                    @Override
                    public void handleDataDeleted(String s) {
                        cdl.countDown();
                    }
                };

                String path = nodePath + genNodeName(theNodeToWaitSignal, false);
                zkClient.subscribeDataChanges(path, zkDataListener);

                if (!zkClient.exists(path)) {
                    cdl.countDown();
                }

                try {
                    cdl.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                threadKeeper.get(Thread.currentThread())
                            .setSuccessor(true);
                return;
            }
        }
    }

    abstract protected String tryAcquire(Integer generatedNode, Map<String, List<String>> childs);

    void release() {
        Thread currentThread = Thread.currentThread();
        NodeInfo nodeInfo;
        // 如果是重入的话，nodeInfo.incr
        if (threadKeeper.containsKey(currentThread) && (nodeInfo = threadKeeper.get(currentThread)).isSuccessor()) {
            if (nodeInfo.decr()) {
                delNodeFromZk(nodeInfo.originNode);
            }
        } else {
            throw new HighTemplarException("Current Thread has not get the lock before");
        }
    }

    private NodeInfo genNodeFromZk(String nodeName) {
        Integer node;
        String nodeOrigin;
        try {
            nodeOrigin = zkClient.createEphemeralSequential(nodePath + genNodeName(nodeName, true), null);
            node = nodeTranslation(nodeOrigin, nodePath);
        } catch (ZkNoNodeException e) {
            // 首次创建节点由于没有上层节点可能会报错
            zkClient.createPersistent(nodePath, true);
            return genNodeFromZk(nodeName);
        }
        return new NodeInfo(nodeOrigin, node);
    }

    private void delNodeFromZk(String nodePath) {
        zkClient.delete(nodePath);
    }

    private Map<String, List<String>> getChildren() {
        return nodeTranslation(zkClient.getChildren(nodePath));
    }

    public static class NodeInfo {

        String originNode;

        int node;

        boolean successor;

        int counter;

        NodeInfo(String originNode, int node) {
            this.originNode = originNode;
            this.node = node;
        }

        boolean isSuccessor() {
            return successor;
        }

        void setSuccessor(boolean successor) {
            this.successor = successor;
        }

        void incr() {
            counter++;
        }

        boolean decr() {
            counter--;
            return counter == -1;
        }
    }
}