package com.wzl.ct.producer;

import com.wzl.ct.common.bean.Producer;
import com.wzl.ct.producer.bean.LocalFileProducer;
import com.wzl.ct.producer.io.LocalFileDataIn;
import com.wzl.ct.producer.io.LocalFileDataOut;

import java.io.IOException;

/**
 * 启动对象
 */
public class BootStrap {
    public static void main(String[] args) throws Exception {
        if(args.length<2){
            System.out.println("系统参数不正确 请按照指定格式传递：java -jar Producer.jar path1 path2");
            System.exit(1);
        }
        //构建生产者对象
        Producer producer = new LocalFileProducer();
        producer.setIn(new LocalFileDataIn(args[0]));
        producer.setOut(new LocalFileDataOut(args[1]));
//        producer.setIn(new LocalFileDataIn("D:\\360极速浏览器下载\\12_大数据技术之项目：电信客服\\2.资料\\辅助文档\\contact.log"));
//        producer.setOut(new LocalFileDataOut("D:\\360极速浏览器下载\\12_大数据技术之项目：电信客服\\2.资料\\辅助文档\\call.log"));
        //生产数据
        producer.produce();
        //关闭生产者对象
        producer.close();
    }
}
