package com.wzl.ct.producer.bean;

import com.wzl.ct.common.bean.Data;

/**
 * 联系人
 */
public class Contact extends Data {
    private String tel;
    private String name;

    @Override
    public void setValue(Object val){
        content = (String)val;
        String[] values = content.split("\t");
        setName(values[0]);
        setTel(values[1]);
    }

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Contact{" + "tel='" + tel + '\'' + ", name='" + name + '\'' + '}';
    }
}
