package com.wzl.ct.common.constant;

import com.wzl.ct.common.bean.Val;

/**
 * 名称常量枚举类
 */
public enum  Names implements Val {
    NAMESPACE("ct");

    private  String name;

    Names(String name) {
        this.name = name;
    }

    @Override
    public String getValue() {
        return name;
    }

    @Override
    public void setValue(Object value) {
        this.name = (String) value;
    }
}
