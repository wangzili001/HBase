package com.wzl.ct.consumer.bean;

import com.wzl.ct.common.bean.Consumer;
import com.wzl.ct.common.constant.Names;
import com.wzl.ct.consumer.dao.HBaseDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * 通话日志的消费者对象
 */
public class CalllogConsumer implements Consumer {
    /**
     * 消费数据
     */
    public void consume() {
        try {
            //创建配置对象
            Properties prop = new Properties();
            prop.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("consumer.properties"));
            //获取flume采集的数据
            KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(prop);
            //关注主题
            consumer.subscribe(Arrays.asList(Names.TOPIC.getValue()));
            //初始化HBase
            HBaseDao dao = new HBaseDao();
            dao.init();
            //消费数据
            while ( true ){
                ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println(consumerRecord.value());
                    //插入数据
                    dao.insertData(consumerRecord.value());
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    /**
     * 释放资源
     */
    public void close() throws IOException {

    }
}
