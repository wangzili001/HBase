package com.wzl.ct.common.bean;

import java.io.IOException;

/**
 * 消费者接口
 */
public interface Consumer extends Cloneable {
    /**
     * 消费数据
     */
    public void consume();

    /**
     * 释放资源
     */
    public void close() throws IOException;
}
