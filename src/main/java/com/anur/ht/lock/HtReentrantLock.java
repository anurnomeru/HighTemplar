package com.anur.ht.lock;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import com.anur.ht.common.HtZkClient;

/**
 * Created by Anur IjuoKaruKas on 2019/6/19
 */
public class HtReentrantLock extends AbstractZkSynchronizer {

    public HtReentrantLock(String lockName, HtZkClient htZkClient) {
        super(lockName, htZkClient);
    }

    @Override
    protected String tryAcquire(Integer generatedNode, Map<String, List<String>> childs) {
        return Optional.ofNullable(getNodeNextToIfNotMin(generatedNode, childs.values()
                                                                              .stream()
                                                                              .flatMap(Collection::stream)
                                                                              .collect(Collectors.toList())))
                       .map(s -> DEFAULT_NODE_NAME + s)
                       .orElse(null);
    }

    public void lock() {
        acquire(DEFAULT_NODE_NAME);
    }

    public void unLock() {
        release(DEFAULT_NODE_NAME);
    }
}
