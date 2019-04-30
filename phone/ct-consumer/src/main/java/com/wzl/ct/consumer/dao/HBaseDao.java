package com.wzl.ct.consumer.dao;

import com.wzl.ct.common.bean.BaseDao;
import com.wzl.ct.common.constant.Names;
import com.wzl.ct.common.constant.ValueConstant;
import com.wzl.ct.consumer.bean.Calllog;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * HBase数据访问对象
 */
public class HBaseDao extends BaseDao {
    /**
     * 初始化
     */
    public void init() throws IOException {
        start();

        createNamespaceNX(Names.NAMESPACE.getValue());
        createTableXX(Names.TABLE.getValue(), ValueConstant.REGION_COUNT,Names.CF_CALLER.getValue(),Names.CF_CALLED.getValue());

        end();
    }

    /**
     * 插入对象
     * @param
     * @throws Exception
     */
    public void insertData(Calllog log) throws Exception{
        log.setRowkey(genRegionNum(log.getCall1(),log.getCalltime())+"_"+log.getCall1()+"_"+log.getCalltime()+
                "_"+log.getCall2()+"_"+log.getDuration());
        putData(log);
    }

    /**
     * 插入数据
     * @param value
     */
    public void insertData(String value) throws IOException {
        //将通话日志保存到HBase表中
        //1.获取通话日志数据
        String[] split = value.split("\t");
        String call1 =split[0];
        String call2 =split[1];
        String calltime =split[2];
        String duration = split[3];
        //2.创建数据对象
        //rowkey的设计
        //1）长度原则：最大的值为64Kb 推荐长度 10~100字节  最好是8的倍数 能短则短 rowkey太长会影响性能
        //2）唯一原则：rowkey应该具备唯一性
        //3）散列原则：
            // 3-1）盐值散列： 不能使用时间戳直接作为rowkey  在rowkey前增加随机数
            //3-2）字符串的反转：在时间戳或者电话号码用的多 前面有规律 反转后避免数据热点问题
            //3-3）计算分区号：hashmap
        //rowkey=regionNum + call1 + time + call2 + duration
        String rowkey = genRegionNum(call1,calltime)+"_"+call1+"_"+calltime+"_"+call2+"_"+duration+"_1";
        //主叫用户
        Put put = new Put(Bytes.toBytes(rowkey));
        byte[] family = Bytes.toBytes(Names.CF_CALLER.getValue());

        put.addColumn(family,Bytes.toBytes("call1"),Bytes.toBytes(call1));
        put.addColumn(family,Bytes.toBytes("call2"),Bytes.toBytes(call2));
        put.addColumn(family,Bytes.toBytes("calltime"),Bytes.toBytes(calltime));
        put.addColumn(family,Bytes.toBytes("duration"),Bytes.toBytes(duration));
        put.addColumn(family,Bytes.toBytes("flag"),Bytes.toBytes("1"));

        String calledrowkey = genRegionNum(call2,calltime)+"_"+call2+"_"+calltime+"_"+call1+"_"+duration+"_0";

        //被叫用户
//        Put calledPut = new Put(Bytes.toBytes(calledrowkey));

//        byte[] calledfamily = Bytes.toBytes(Names.CF_CALLER.getValue());
//
//        calledPut.addColumn(calledfamily,Bytes.toBytes("call1"),Bytes.toBytes(call2));
//        calledPut.addColumn(calledfamily,Bytes.toBytes("call2"),Bytes.toBytes(call1));
//        calledPut.addColumn(calledfamily,Bytes.toBytes("calltime"),Bytes.toBytes(calltime));
//        calledPut.addColumn(calledfamily,Bytes.toBytes("duration"),Bytes.toBytes(duration));
//        calledPut.addColumn(calledfamily,Bytes.toBytes("flag"),Bytes.toBytes("0"));
        List<Put> puts = new ArrayList<Put>();
        puts.add(put);
//        puts.add(calledPut);
        //3.保存数据
        putData(Names.TABLE.getValue(),puts);
    }
}
