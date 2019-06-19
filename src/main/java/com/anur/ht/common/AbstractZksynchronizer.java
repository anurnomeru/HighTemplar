package com.anur.ht.common;

import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
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

    private ConcurrentHashMap<Thread, NodeInfo> pathKeeper = new ConcurrentHashMap<>();

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
    void acquire(String specialSign) {
        Thread currentThread = Thread.currentThread();
        NodeInfo nodeInfo;

        // 如果是重入的话，nodeInfo.incr
        if (pathKeeper.containsKey(currentThread) && (nodeInfo = pathKeeper.get(currentThread)).isSuccessor()) {
            nodeInfo.incr();
            return;
        }

        // 从 zkClient 中生成一个当前节点的 node
        nodeInfo = genNode(specialSign);
        pathKeeper.put(currentThread, nodeInfo);
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

                String path = nodePath + NODE_PATH_SEPARATOR + theNodeToWaitSignal;
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
                pathKeeper.get(Thread.currentThread())
                          .setSuccessor(true);
                return;
            }
        }
    }

    abstract protected String tryAcquire(Integer generatedNode, Map<String, Optional<String>> minimumChild);

    void release() {
        Thread currentThread = Thread.currentThread();
        NodeInfo nodeInfo;
        // 如果是重入的话，nodeInfo.incr
        if (pathKeeper.containsKey(currentThread) && (nodeInfo = pathKeeper.get(currentThread)).isSuccessor()) {
            if (nodeInfo.decr()) {
                delNode(nodeInfo.originNode);
            }
        } else {
            throw new HighTemplarException("Current Thread has not get the lock before");
        }
    }

    private NodeInfo genNode(String specialSign) {
        Integer node;
        String nodeOrigin;
        try {
            nodeOrigin = zkClient.createEphemeralSequential(nodePath + genNodeName(specialSign), null);
            node = nodeTranslation(nodeOrigin, nodePath);
        } catch (ZkNoNodeException e) {
            // 首次创建节点由于没有上层节点可能会报错
            zkClient.createPersistent(nodePath, true);
            return genNode(specialSign);
        }
        return new NodeInfo(nodeOrigin, node);
    }

    private void delNode(String nodePath) {
        zkClient.delete(nodePath);
    }

    private Map<String, Optional<String>> getChildren() {
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

    public static class Mutex extends AbstractZksynchronizer {

        public Mutex(String lockName, ZkClient zkClient) {
            super(lockName, zkClient);
        }

        @Override
        protected String tryAcquire(Integer generatedNode, Map<String, Optional<String>> minimumChild) {
            Entry<String, Optional<String>> minEntry = minimumChild.entrySet()
                                                                   .stream()
                                                                   .min(Comparator.comparing(o -> Integer.valueOf(o.getValue()
                                                                                                                   .get())))
                                                                   .orElse(null);
            return Optional.ofNullable(minEntry)
                           .map(e -> Integer.valueOf(e.getValue()
                                                      .get())
                                            .compareTo(generatedNode) >= 0)
                           .orElse(true) ? null : minEntry.getKey() + minEntry.getValue()
                                                                              .get();
        }

        public void lock() {
            acquire(null);
        }

        public void unLock() {
            release();
        }

        public void lock(String specialSign) {
            acquire(specialSign);
        }
    }

    public static void main(String[] args) {

        Runnable runnable = () -> {
            Mutex mutex = new Mutex("Anur-Test", new ZkClient("127.0.0.1"));
            mutex.lock();
            System.out.println(Thread.currentThread() + "获取到锁啦！！");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(Thread.currentThread() + "解锁啦！！");
            mutex.release();
        };
        new Thread(runnable).start();
        new Thread(runnable).start();
    }
}