package com.anur.ht.lock;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.I0Itec.zkclient.ZkClient;

/**
 * Created by Anur IjuoKaruKas on 2019/6/19
 */
public class HtReentrantLock extends AbstractZksynchronizer {

    public HtReentrantLock(String lockName, ZkClient zkClient) {
        super(lockName, zkClient);
    }

    @Override
    protected String tryAcquire(Integer generatedNode, Map<String, List<String>> childs) {
        return getNodeNextToIfNotMin(generatedNode, childs.values()
                                                          .stream()
                                                          .flatMap(Collection::stream)
                                                          .collect(Collectors.toList()));
    }

    public void lock() {
        acquire(null);
    }

    public void unLock() {
        release();
    }
}