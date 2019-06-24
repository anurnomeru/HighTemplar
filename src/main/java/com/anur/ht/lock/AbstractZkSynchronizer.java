package com.anur.ht.lock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.ht.common.HtZkClient;
import com.anur.ht.exception.HighTemplarException;

/**
 * Created by Anur IjuoKaruKas on 2019/6/16
 */
public abstract class AbstractZkSynchronizer extends NodeOperator {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractZkSynchronizer.class);

    static final List<String> EMPTY_LIST = new ArrayList<>();

    /**
     * we use this path to acquire ephemeral sequential
     */
    String pathPrefix;

    /**
     * htZkClient is a zkClient generator
     */
    HtZkClient htZkClient;

    /**
     * threadKeeper hold the NodeInfo which store the lock info during the AZS
     */
    ConcurrentHashMap<Thread, NodeInfo> threadKeeper;

    /**
     * Please make sure that different lock has uniquely lockName, we
     * define same lock in cluster with same lockName.
     *
     * 确保不同的锁使用不同的 lockName，同一个锁则依靠同一锁名来进行同步控制
     */
    public AbstractZkSynchronizer(String lockName, HtZkClient htZkClient) {
        this.pathPrefix = genNodePath(lockName);
        this.htZkClient = htZkClient;
        this.threadKeeper = new ConcurrentHashMap<>();
    }

    /**
     * nodeName 的长度我们规定为 8（包含开头的 '/'），这是为了获取 children 时方便截取，默认传入 {@link #DEFAULT_NODE_NAME} 即可
     *
     * we rule that 'nodeName's length must be 8 (contain the first char '/') which helping
     * {@link #nodeTranslation(String, String)} to specify `nodeName` and `ephemeral sequential`.
     */
    void acquire(String nodeName) {
        nodeName = genNodeName(nodeName, true);

        Thread currentThread = Thread.currentThread();
        NodeInfo nodeInfo;

        if (threadKeeper.containsKey(currentThread)) {
            if ((nodeInfo = threadKeeper.get(currentThread)
                                        .getByNodeName(nodeName)) != null
                && nodeInfo.isSuccessor()) {
                int count = nodeInfo.incr(); // reentrant

                LOG.info("{}/{} reentrant acquire, now counter is {}", pathPrefix, nodeInfo.pathSuffix, count);
                return;
            } else {
                threadKeeper.get(currentThread)
                            .addNodeInfo(nodeInfo = genNodeFromZk(nodeName));

                LOG.warn("{}/{} try to acquire with another nodeName, and it may lead to DEAD-LOCK!", pathPrefix, nodeInfo.pathSuffix);
            }
        } else {
            nodeInfo = genNodeFromZk(nodeName);
            threadKeeper.put(currentThread, nodeInfo);
            LOG.info("{}/{} try to acquire", pathPrefix, nodeInfo.pathSuffix);
        }
        acquireQueue(nodeInfo.zkClient, nodeInfo);
    }

    /**
     * The release semantics are the same as AQS, how many times we acquire then
     * we should release same times or else the lock will not fully release.
     *
     * 释放语义与 AQS 相同， acquire 多少次，相对的也要 release 多少次。
     */
    void release(String nodeName) {
        nodeName = genNodeName(nodeName, true);

        Thread currentThread = Thread.currentThread();
        NodeInfo nodeInfo;
        // 如果是重入的话，nodeInfo.incr
        if (threadKeeper.containsKey(currentThread)
            && (nodeInfo = threadKeeper.get(currentThread)
                                       .getByNodeName(nodeName)).isSuccessor()) {

            int count = nodeInfo.decr();
            LOG.info("{}/{} try to release, now counter is {}", pathPrefix, nodeInfo.pathSuffix, count);

            if (count == 0) {
                LOG.info("{}/{} release success.", pathPrefix, nodeInfo.pathSuffix);

                delNodeFromZk(nodeInfo.zkClient, nodeInfo.fullPath);
                if (threadKeeper.get(currentThread)
                                .remove(nodeName)) {
                    threadKeeper.remove(currentThread);

                    LOG.info("has been completed release the lock");
                } else {
                    LOG.info("but current thread may hold the lock or waiting for the lock");
                }
            }
        } else {
            throw new HighTemplarException("Current Thread has not get the lock before");
        }
    }

    /**
     * get all the child that under the node's path to judge if current thread need park,
     * we should override the {@link #tryAcquire(Integer, Map)} method, return 'null'
     * mean current thread obtain the lock, or return the child name should waiting for.
     *
     * 这里面其实是一个阻塞获取路径下所有其他节点，并以此节点来判断是否需要阻塞的方法。
     * 子类需要重写 {@link #tryAcquire(Integer, Map)}， 返回空代表无需阻塞，
     * 否则返回需要等待通知的节点的名字。
     */
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
                LOG.info("{}/{} is waiting for {} to signal", pathPrefix, nodeInfo.pathSuffix, path);

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
                LOG.info("{}/{} acquire success!", pathPrefix, nodeInfo.pathSuffix);
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
        nodeInfo.counter = 1;

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

        int incr() {
            return ++counter;
        }

        int decr() {
            return --counter;
        }

        /**
         * return if NodeInfo (List) is Empty
         */
        boolean remove(String nodeName) {
            if (this.next == null) {
                return true;
            } else {
                removeRecursive(nodeName);
                return false;
            }
        }

        private void removeRecursive(String nodeName) {
            boolean hasNext = (this.next != null);

            if (hasNext) {
                if (this.nodeName.equals(nodeName)) {
                    this.nodeName = this.next.nodeName;
                    this.zkClient = this.next.zkClient;
                    this.node = this.next.node;
                    this.successor = this.next.successor;
                    this.counter = this.next.counter;
                    this.next = this.next.next;
                } else {
                    this.next.removeRecursive(nodeName);
                }
            } else {
                throw new HighTemplarException("Current Thread has not get the lock before");
            }
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