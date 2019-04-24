package com.wzl.ct.common.bean;

/**
 * 数据对象
 */
public abstract class Data implements Val {
    public String content;

    public String getValue() {
        return content;
    }

    public void setValue(Object value) {
        content = (String) value;
    }
}
