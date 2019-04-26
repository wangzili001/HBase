package com.wzl.ct.consumer;

import com.wzl.ct.common.bean.Consumer;
import com.wzl.ct.consumer.bean.CalllogConsumer;

import java.io.IOException;

/**
 * 使用kafka消费者获取Flume采集的数据
 * 将数据存储到HBase中
 */
public class BootStrap {
    public static void main(String[] args) throws IOException {
        //创建消费者
        Consumer consumer = new CalllogConsumer();
        //消费数据
        consumer.consume();
        //关闭资源
        consumer.close();
    }
}
