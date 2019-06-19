package com.anur.ht.lock;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
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

    protected final static String NODE_PATH_SEPARATOR = "/";

    private final static String DEFAULT_LOCK_NAME = "/DEF-LK";

    private final static String DEFAULT_SPECIAL_SIGN = "/DEF-LK";

    private final static int SPECIAL_SIGN_LENGTH = 7;

    String genNodePath(String nodePath) {
        return NODE_PATH_PREFIX + Optional.ofNullable(nodePath)
                                          .map(s -> s.startsWith(NODE_PATH_SEPARATOR) ? s : NODE_PATH_SEPARATOR + s)
                                          .orElse(DEFAULT_LOCK_NAME);
    }

    String genNodeName(String nodeName, boolean lengthCheck) {
        return nodeName == null
            ? DEFAULT_SPECIAL_SIGN
            : Optional.of(nodeName)
                      .map(s -> s.startsWith(NODE_PATH_SEPARATOR) ? s : NODE_PATH_SEPARATOR + s)
                      .filter(s -> !lengthCheck || s.length() == SPECIAL_SIGN_LENGTH)
                      .orElseThrow(() ->
                          new HighTemplarException("nodeName's length must be " +
                              SPECIAL_SIGN_LENGTH + " , for example '/DEF-LK', if nodeName not start with '/', high templar will add it to the beginning."));
    }

    Integer nodeTranslation(String node, String nodePath) {
        int pathLength = nodePath.length() + SPECIAL_SIGN_LENGTH;
        return Optional.of(node)
                       .map(s -> s.substring(pathLength))
                       .map(Integer::new)
                       .orElseThrow(() -> new HighTemplarException("fail to cast str node: " + node + " to Integer"));
    }

    Map<String, Optional<String>> nodeTranslation(List<String> nodes) {
        return nodes.stream()
                    .collect(Collectors.groupingBy(s -> s.substring(0, SPECIAL_SIGN_LENGTH - 1),
                        Collectors.mapping(s -> s.substring(SPECIAL_SIGN_LENGTH - 1), Collectors.minBy(Comparator.comparing(Integer::valueOf)))
                    ));
    }
}
