package com.anur.ht.lock;

import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import org.I0Itec.zkclient.ZkClient;

/**
 * Created by Anur IjuoKaruKas on 2019/6/19
 */
public class HtReentrantLock extends AbstractZksynchronizer {

    public HtReentrantLock(String lockName, ZkClient zkClient) {
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
}
