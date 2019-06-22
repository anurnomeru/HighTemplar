package com.anur.ht.lock;

import java.util.ArrayList;
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
    String pathPrefix;

    HtZkClient htZkClient;

    ConcurrentHashMap<Thread, NodeInfo> threadKeeper;

    /**
     * Please make sure that different lock has uniquely lockName, we
     * define same lock in cluster with same lockName.
     *
     * 确保不同的锁使用不同的 lockName，同一个锁则依靠同一锁名来进行同步控制
     */
    public AbstractZksynchronizer(String lockName, HtZkClient htZkClient) {
        this.pathPrefix = genNodePath(lockName);
        this.htZkClient = htZkClient;
        this.threadKeeper = new ConcurrentHashMap<>();
    }

    /**
     * specialSign 的长度我们规定为 6，这是为了获取 children 时方便截取，默认传入 null 即可
     */
    void acquire(String nodeName) {
        nodeName = genNodeName(nodeName, true);

        Thread currentThread = Thread.currentThread();
        NodeInfo nodeInfo;

        if (threadKeeper.containsKey(currentThread)) {
            if ((nodeInfo = threadKeeper.get(currentThread)
                                        .getByNodeName(nodeName)) != null
                && nodeInfo.isSuccessor()) {
                nodeInfo.incr(); // reentrant
                return;
            } else {
                threadKeeper.get(currentThread)
                            .addNodeInfo(nodeInfo = genNodeFromZk(nodeName));
            }
        } else {
            nodeInfo = genNodeFromZk(nodeName);
            threadKeeper.put(currentThread, nodeInfo);
        }
        acquireQueue(nodeInfo.zkClient, nodeInfo);
    }

    void release(String nodeName) {
        nodeName = genNodeName(nodeName, true);

        Thread currentThread = Thread.currentThread();
        NodeInfo nodeInfo;
        // 如果是重入的话，nodeInfo.incr
        if (threadKeeper.containsKey(currentThread)
            && (nodeInfo = threadKeeper.get(currentThread)
                                       .getByNodeName(nodeName)).isSuccessor()) {

            if (nodeInfo.decr()) {
                delNodeFromZk(nodeInfo.zkClient, nodeInfo.fullPath);
                if (threadKeeper.get(currentThread)
                                .remove(nodeName)) {
                    threadKeeper.remove(currentThread);
                }
            }
        } else {
            throw new HighTemplarException("Current Thread has not get the lock before");
        }
    }

    private void acquireQueue(ZkClient zkClient, NodeInfo nodeInfo) {
        for (; ; ) {
            String theNodeToWaitSignal;

            // 由子类判断是否成功获取锁
            if ((theNodeToWaitSignal = tryAcquire(nodeInfo.node,
                nodeTranslation(threadKeeper.get(Thread.currentThread()).zkClient.getChildren(pathPrefix), threadKeeper.get(Thread.currentThread())
                                                                                                                       .getSuffixRecursive())
            )) != null) {

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

                String path = pathPrefix + genNodeName(theNodeToWaitSignal, false);

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
                nodeInfo.successor = true;
                return;
            }
        }
    }

    abstract protected String tryAcquire(Integer generatedNode, Map<String, List<String>> childs);

    private NodeInfo genNodeFromZk(String nodeName) {
        NodeInfo nodeInfo = new NodeInfo();
        ZkClient client = htZkClient.gen();
        nodeInfo.nodeName = nodeName;
        nodeInfo.zkClient = client;

        try {
            nodeInfo.fullPath = client.createEphemeralSequential(pathPrefix + nodeName, null);
            nodeInfo.node = nodeTranslation(nodeInfo.fullPath, pathPrefix);
        } catch (ZkNoNodeException e) {
            // 首次创建节点由于没有上层节点可能会报错
            client.createPersistent(pathPrefix, true);
            return genNodeFromZk(nodeName);
        }

        nodeInfo.pathSuffix = nodeInfo.fullPath.substring(
            nodeInfo.fullPath.indexOf(nodeName) + 1
        );

        return nodeInfo;
    }

    private void delNodeFromZk(ZkClient zkClient, String nodePath) {
        zkClient.delete(nodePath);
    }

    public static class NodeInfo {

        ZkClient zkClient;

        String nodeName;

        String pathSuffix;

        String fullPath;

        int node;

        boolean successor;

        int counter;

        private NodeInfo next;

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

        /**
         * return if NodeInfo (List) is Empty
         */
        boolean remove(String nodeName) {
            boolean removeThis = false;

            if (this.next != null) {
                if (removeThis = this.nodeName.equals(nodeName)) {
                    this.nodeName = this.next.nodeName;
                    this.zkClient = this.next.zkClient;
                    this.node = this.next.node;
                    this.successor = this.next.successor;
                    this.counter = this.next.counter;
                    this.next = this.next.next;
                } else {
                    remove(this.next.nodeName);
                }
            }

            return removeThis && this.next == null;
        }

        void addNodeInfo(NodeInfo nodeInfo) {
            if (next == null) {
                next = nodeInfo;
            } else {
                next.addNodeInfo(nodeInfo);
            }
        }

        NodeInfo getByNodeName(String nodeName) {
            return this.nodeName.equals(nodeName) ? this : next == null ? null : next.getByNodeName(nodeName);
        }

        public List<String> getSuffixRecursive() {
            List<String> list = new ArrayList<>();
            NodeInfo nodeInfo = this;
            do {
                list.add(pathSuffix);
                nodeInfo = nodeInfo.next;
            } while (nodeInfo != null);

            return list;
        }
    }
}