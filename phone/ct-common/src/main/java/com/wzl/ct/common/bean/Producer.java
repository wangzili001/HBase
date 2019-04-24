package com.wzl.ct.common.bean;

import java.io.Closeable;

/**
 * 生产者接口
 */
public interface Producer extends Closeable {
    /**
     * 生产数据
     */
    public void produce();

    public void setIn(DataIn dataIn);
    public void setOut(DataOut dataOut);
}
