package com.anur.ht.lock;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import com.anur.ht.common.HtZkClient;

/**
 * Created by Anur IjuoKaruKas on 2019/6/19
 */
public class HtReentrantReadWriteLock {

    private static final String READ_LOCK_NODE_NAME = "/READ-LK";

    private static final String WRITE_LOCK_NODE_NAME = "/WRIT-LK";

    private ReadLock readLock;

    private WriteLock writeLock;

    public HtReentrantReadWriteLock(String lockName, HtZkClient htZkClient) {
        writeLock = new WriteLock(lockName, htZkClient);
        readLock = new ReadLock(lockName, htZkClient, writeLock.threadKeeper);
    }

    public ReadLock readLock() {
        return readLock;
    }

    public WriteLock writeLock() {
        return writeLock;
    }

    public static class WriteLock extends AbstractZkSynchronizer {

        private WriteLock(String lockName, HtZkClient htZkClient) {
            super(lockName, htZkClient);
        }

        @Override
        protected String tryAcquire(Integer generatedNode, Map<String, List<String>> childs) {

            Optional<String> minWriteLock = Optional.ofNullable(getNodeNextToIfNotMin(generatedNode, childs.getOrDefault(WRITE_LOCK_NODE_NAME, EMPTY_LIST)));
            Optional<String> minReadLock = Optional.ofNullable(getNodeNextToIfNotMin(generatedNode, childs.getOrDefault(READ_LOCK_NODE_NAME, EMPTY_LIST)));

            if (minReadLock.isPresent() && minWriteLock.isPresent()) {
                String mwl = minWriteLock.get();
                String mrl = minReadLock.get();

                return Integer.valueOf(mwl) > Integer.valueOf(mrl) ?
                    READ_LOCK_NODE_NAME + mrl :
                    WRITE_LOCK_NODE_NAME + mwl;
            } else {
                String needToWait = minWriteLock.map(s -> WRITE_LOCK_NODE_NAME + s)
                                                .orElse(null);
                needToWait = minReadLock.map(s -> READ_LOCK_NODE_NAME + s)
                                        .orElse(needToWait);
                return needToWait;
            }
        }

        public void lock() {
            acquire(WRITE_LOCK_NODE_NAME);
        }

        public void unLock() {
            release(WRITE_LOCK_NODE_NAME);
        }
    }

    public static class ReadLock extends AbstractZkSynchronizer {

        private ReadLock(String lockName, HtZkClient htZkClient) {
            super(lockName, htZkClient);
        }

        private ReadLock(String lockName, HtZkClient htZkClient, ConcurrentHashMap<Thread, NodeInfo> threadKeeper) {
            super(lockName, htZkClient);
            this.threadKeeper = threadKeeper;
        }

        @Override
        protected String tryAcquire(Integer generatedNode, Map<String, List<String>> childs) {
            return Optional.ofNullable(getNodeNextToIfNotMin(generatedNode, childs.getOrDefault(WRITE_LOCK_NODE_NAME, EMPTY_LIST)))
                           .map(s -> WRITE_LOCK_NODE_NAME + s)
                           .orElse(null);
        }

        public void lock() {
            acquire(READ_LOCK_NODE_NAME);
        }

        public void unLock() {
            release(READ_LOCK_NODE_NAME);
        }
    }
}
