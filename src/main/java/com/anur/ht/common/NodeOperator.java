package com.anur.ht.common;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import com.anur.ht.exception.HighTemplarException;

/**
 * Created by Anur IjuoKaruKas on 2019/6/17
 */
public class NodeOperator {

    /**
     * Avoid using this path for other purposes when using zookeeper
     *
     * 使用 zookeeper 时避免此路径作他用
     */
    private final static String NODE_PATH_PREFIX = "/HIGH-TEMPLAR";

    private final static String NODE_PATH_SUFFIX = "/";

    private final static String DEFAULT_SPECIAL_SIGN = "DEF-LK";

    private final static int SPECIAL_SIGN_LENGTH = 6;

    protected String genNodePath(String lockName) {
        return lockName.endsWith(NODE_PATH_SUFFIX) ? lockName : (lockName + NODE_PATH_SUFFIX);
    }

    protected String genNodeName(String specialSign) {
        return Optional.ofNullable(specialSign)
                       .filter(s -> s.length() == SPECIAL_SIGN_LENGTH)
                       .orElse(DEFAULT_SPECIAL_SIGN);
    }

    protected Integer nodeTranslation(String node) {
        return Optional.of(node)
                       .map(s -> s.substring(SPECIAL_SIGN_LENGTH))
                       .map(Integer::new)
                       .orElseThrow(() -> new HighTemplarException("fail to cast str node: " + node + " to Integer"));
    }

    protected List<Integer> nodeTranslation(List<String> nodes) {
        return nodes.stream()
                    .map(s -> s.substring(SPECIAL_SIGN_LENGTH))
                    .map(Integer::new)
                    .sorted()
                    .collect(Collectors.toList());
    }
}
