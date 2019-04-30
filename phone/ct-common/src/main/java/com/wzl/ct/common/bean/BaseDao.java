package com.wzl.ct.common.bean;

import com.wzl.ct.common.Util.DateUtil;
import com.wzl.ct.common.api.Colume;
import com.wzl.ct.common.api.Rowkey;
import com.wzl.ct.common.api.TableRef;
import com.wzl.ct.common.constant.ValueConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

/**
 * 基础数据访问对象
 */
public abstract class BaseDao {
    private ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();
    private ThreadLocal<Admin> adminHolder = new ThreadLocal<Admin>();

    /**
     * 获取连接对象
     */
    protected synchronized Connection getConnection() throws IOException {
        Connection conn = connHolder.get();
        if(conn == null){
            Configuration conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }
        return conn;
    }
    /**
     * 获取管理对象
     */
    protected synchronized Admin getAdmin() throws IOException {
        Admin admin = adminHolder.get();
        if(admin == null){
            admin = getConnection().getAdmin();
        }
        return admin;
    }

    protected void start() throws IOException {
        getConnection();
        getAdmin();
    }
    protected void end() throws IOException {
        Admin admin = getAdmin();
        Connection connection = getConnection();
        if(admin != null){
            admin.close();
            adminHolder.remove();
        }
        if(connection != null){
            connection.close();
            connHolder.remove();
        }
    }

    /**
     * 创建命名空间 如果存在不需要创建 否则 创建新的
     * @param namespace
     */
    protected void createNamespaceNX(String namespace) throws IOException {
        Admin admin = getAdmin();
        try {
            admin.getNamespaceDescriptor(namespace);
        }catch (NamespaceNotFoundException e){
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDescriptor);
        }
    }

    /**
     * 创建表 如果存在删除表 再创建
     * @throws IOException
     */
    protected void createTableXX(String name,String... families) throws IOException{
        //创建表
        createTableXX(name,null,families);
    }
    /**
     * 创建表 如果存在删除表 再创建
     * @throws IOException
     */
    protected void createTableXX(String name,Integer regionCount,String... families) throws IOException{
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);
        if( admin.tableExists(tableName) ){
            //表存在 删除表
            deleteTabel(name);
        }
        //创建表
        createTable(name,regionCount,families);
    }

    /**
     * 创建表
     * @param name
     * @param families
     * @throws IOException
     */
    private void createTable(String name,Integer regionCount,String... families) throws IOException{
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        if(families.length == 0||families == null){
            families = new String[1];
            families[0] = "info";
        }
        for (String family : families) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
            tableDescriptor.addFamily(columnDescriptor);
        }
        //增加预分区
        if(regionCount == null||regionCount <= 1){
            admin.createTable(tableDescriptor);
        }else {
            //分区键
            byte[][] splitKeys = genSplitKeys(regionCount);
            admin.createTable(tableDescriptor,splitKeys);
        }
    }

    /**
     * 增加对象 自动封装数据 将数据之间保存到hbase中去
     * @param obj
     * @throws Exception
     */
    protected void putData(Object obj) throws Exception{
        //反射
        Class clazz = obj.getClass();
        TableRef tableRef = (TableRef) clazz.getAnnotation(TableRef.class);
        String tableName = tableRef.value();
        //获取表对象

        Field[] fs = clazz.getDeclaredFields();
        String stringRowKey = "";
        for (Field f : fs) {
            Rowkey rowkey = f.getAnnotation(Rowkey.class);
            if(rowkey != null){
                f.setAccessible(true);
                stringRowKey = (String)f.get(obj);
                break;
            }
        }
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(stringRowKey));
        for (Field f : fs) {
            Colume colume = f.getAnnotation(Colume.class);
            if(colume != null){
                String family = colume.family();
                String colName = colume.column();
                if(colName == null || "".equals(colName)){
                    colName = f.getName();
                }
                f.setAccessible(true);
                String value = (String)f.get(obj);
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(colName),Bytes.toBytes(value));
            }
        }
        //增加数据
        table.put(put);
        //关闭表
        table.close();
    }

    /**
     * 增加数据
     * @param name
     * @param put
     */
    protected void putData(String name, Put put) throws  IOException{
        //获取表对象
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(name));
        //增加数据
        table.put(put);
        //关闭表
        table.close();
    }
    /**
     * 增加多条数据
     * @param name
     * @param puts
     */
    protected void putData(String name, List<Put> puts) throws  IOException{
        //获取表对象
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(name));
        //增加数据
        table.put(puts);
        //关闭表
        table.close();
    }
    /**
     * 删除表
     * @param name
     * @throws IOException
     */
    protected void deleteTabel(String name) throws IOException{
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    /**
     * 获取查询时startrow,stoprow集合
     * @return
     */
    protected List<String[]> getStartRowkeys(String tel,String start,String end){
        List<String[]> rowkeyss = new ArrayList<String[]>();
        String startTime = start.substring(0,6);
        String endTime = end.substring(0,6);

        Calendar startCal = Calendar.getInstance();
        startCal.setTime(DateUtil.parse(startTime,"yyyyMM"));
        Calendar endCal = Calendar.getInstance();
        endCal.setTime(DateUtil.parse(endTime,"yyyyMM"));

        while (startCal.getTimeInMillis() <= endCal.getTimeInMillis()){
            //当前时间
            String nowTime = DateUtil.format(startCal.getTime(),"yyyyMM");

            int regionNum = genRegionNum(tel,startTime);

            String startRow = regionNum+"_"+tel+"_"+nowTime;
            String stopRow = startRow + "|";
            String[] rowkeys = {startRow,stopRow};
            rowkeyss.add(rowkeys);
            //月份+1
            startCal.add(Calendar.MONTH,1);
        }

        return rowkeyss;
    }
    /**
     * 计算分区号
     * @return
     */
    protected int genRegionNum(String tel,String date){
        String userCode = tel.substring(tel.length()-4);
        String yearMonth = date.substring(0,6);
        int userCodeHash = userCode.hashCode();
        int yearMonthHash = yearMonth.hashCode();
        //crc校验采用亦或算法
        int crc = Math.abs(userCodeHash^yearMonthHash);
        //取模
        int regionNum = crc % ValueConstant.REGION_COUNT;
        return regionNum;
    }
    /**
     * 生成分区键
     * @param regionCount
     * @return
     */
    private byte[][] genSplitKeys(int regionCount){
        int splitKeyCount = regionCount - 1;
        byte[][] bs = new byte[splitKeyCount][];
        List<byte[]> bsList = new ArrayList<byte[]>();
        for (int i = 0; i < splitKeyCount; i++) {
            String splitKey = i + "|";
            bsList.add(Bytes.toBytes(splitKey));
        }
        bsList.toArray(bs);
        return bs;
    }
}
