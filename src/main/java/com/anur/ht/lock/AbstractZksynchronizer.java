package com.anur.ht.lock;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import com.anur.ht.common.HtZkClient;
import com.anur.ht.exception.HighTemplarException;

/**
 * Created by Anur IjuoKaruKas on 2019/6/16
 */
public abstract class AbstractZksynchronizer extends NodeOperator {

    /**
     * we use this path to acquire ephemeral sequential
     */
    private String nodePath;

    private HtZkClient htZkClient;

    private ConcurrentHashMap<Thread, NodeInfo> threadKeeper = new ConcurrentHashMap<>();

    /**
     * Please make sure that different lock has uniquely lockName, we
     * define same lock in cluster with same lockName.
     *
     * 确保不同的锁使用不同的 lockName，同一个锁则依靠同一锁名来进行同步控制
     */
    public AbstractZksynchronizer(String lockName, HtZkClient htZkClient) {
        this.nodePath = genNodePath(lockName);
        this.htZkClient = htZkClient;
    }

    /**
     * specialSign 的长度我们规定为 6，这是为了获取 children 时方便截取
     */
    void acquire(String nodeName) {
        Thread currentThread = Thread.currentThread();
        NodeInfo nodeInfo;

        // 如果是重入的话，nodeInfo.incr
        if (threadKeeper.containsKey(currentThread)
            && (nodeInfo = threadKeeper.get(currentThread)).isSuccessor()
            && nodeInfo.counter > -1) {
            nodeInfo.incr();
            return;
        }

        // 从 zkClient 中生成一个当前节点的 node
        nodeInfo = genNodeFromZk(nodeName);
        threadKeeper.put(currentThread, nodeInfo);
        acquireQueue(nodeInfo.node);
    }

    private void acquireQueue(int node) {
        Thread currentThread = Thread.currentThread();
        NodeInfo nodeInfo = threadKeeper.get(currentThread);
        ZkClient client = nodeInfo.zkClient;

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

                client.subscribeDataChanges(path, zkDataListener);

                if (!client.exists(path)) {
                    cdl.countDown();
                }

                try {
                    cdl.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                nodeInfo.successor = true;
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
        NodeInfo nodeInfo = new NodeInfo();
        ZkClient client = htZkClient.gen();
        nodeInfo.nodeName = nodeName;
        nodeInfo.zkClient = client;

        try {
            nodeInfo.originNode = client.createEphemeralSequential(nodePath + genNodeName(nodeName, true), null);
            nodeInfo.node = nodeTranslation(nodeInfo.originNode, nodePath);
        } catch (ZkNoNodeException e) {
            // 首次创建节点由于没有上层节点可能会报错
            client.createPersistent(nodePath, true);
            return genNodeFromZk(nodeName);
        }

        return nodeInfo;
    }

    private void delNodeFromZk(String nodePath) {
        threadKeeper.get(Thread.currentThread()).zkClient.delete(nodePath);
    }

    private Map<String, List<String>> getChildren() {
        return nodeTranslation(threadKeeper.get(Thread.currentThread()).zkClient.getChildren(nodePath));
    }

    public static class NodeInfo {

        String nodeName;

        ZkClient zkClient;

        String originNode;

        int node;

        boolean successor;

        int counter;

        boolean isSuccessor() {
            return successor;
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