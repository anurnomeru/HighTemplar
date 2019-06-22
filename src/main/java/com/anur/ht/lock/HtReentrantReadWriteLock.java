package com.anur.ht.lock;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import com.anur.ht.common.HtZkClient;

/**
 * Created by Anur IjuoKaruKas on 2019/6/19
 */
public class HtReentrantReadWriteLock {

    private static final String READ_LOCK_NODE_NAME = "/HT-REL";

    private static final String WRITE_LOCK_NODE_NAME = "/HT-WRL";

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

    public static class WriteLock extends AbstractZksynchronizer {

        private WriteLock(String lockName, HtZkClient htZkClient) {
            super(lockName, htZkClient);
        }

        @Override
        protected String tryAcquire(Integer generatedNode, Map<String, List<String>> childs) {
            return getNodeNextToIfNotMin(generatedNode, childs.values()
                                                              .stream()
                                                              .flatMap(Collection::stream)
                                                              .collect(Collectors.toList()));
        }

        public void lock() {
            acquire(WRITE_LOCK_NODE_NAME);
        }

        public void unLock() {
            release(WRITE_LOCK_NODE_NAME);
        }
    }

    public static class ReadLock extends AbstractZksynchronizer {

        private ReadLock(String lockName, HtZkClient htZkClient) {
            super(lockName, htZkClient);
        }

        private ReadLock(String lockName, HtZkClient htZkClient, ConcurrentHashMap<Thread, NodeInfo> threadKeeper) {
            super(lockName, htZkClient);
            this.threadKeeper = threadKeeper;
        }

        @Override
        protected String tryAcquire(Integer generatedNode, Map<String, List<String>> childs) {
            return getNodeNextToIfNotMin(generatedNode,
                Optional.ofNullable(childs.get(WRITE_LOCK_NODE_NAME))
                        .orElse(Collections.EMPTY_LIST));
        }

        public void lock() {
            acquire(READ_LOCK_NODE_NAME);
        }

        public void unLock() {
            release(READ_LOCK_NODE_NAME);
        }
    }
}
