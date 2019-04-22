package com.wzl.bigtable.hos.server;

import com.wzl.bigtable.hos.core.ErrorCodes;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HBaseServiceImpl {
    //创建表
    public static boolean createTable(Connection connection, String tableName, String[] cfs, byte[][] splitKeys) {
        try (HBaseAdmin admin = (HBaseAdmin) connection.getAdmin()) {
            if (admin.tableExists(tableName)) {
                return false;
            }
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            Arrays.stream(cfs).forEach(cf -> {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
                hColumnDescriptor.setMaxVersions(1);
                hTableDescriptor.addFamily(hColumnDescriptor);
            });
            admin.createTable(hTableDescriptor, splitKeys);
        } catch (IOException e) {
            e.printStackTrace();
            throw new HosServerException(ErrorCodes.ERROR_HBASE, "create table error");
        }
        return true;
    }

    //删除表
    public static boolean deleTable(Connection connection, String tableName) {
        try (HBaseAdmin admin = (HBaseAdmin) connection.getAdmin()) {
            admin.disableTable(tableName);
            admin.disableTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
            throw new HosServerException(ErrorCodes.ERROR_HBASE, "delete table error");
        }
        return true;
    }

    //删除列族
    public static boolean deleColumFamily(Connection connection, String tableName, String cf) {
        try (HBaseAdmin admin = (HBaseAdmin) connection.getAdmin()) {
            admin.deleteColumn(tableName, cf);
        } catch (IOException e) {
            e.printStackTrace();
            throw new HosServerException(ErrorCodes.ERROR_HBASE, "delete cf error");
        }
        return true;
    }

    //删除列
    public static boolean deleteQualifier(Connection connection, String tableName, String rowKey, String cf, String column) {
        Delete delete = new Delete(rowKey.getBytes());
        delete.addColumn(cf.getBytes(), column.getBytes());
        return deleteRow(connection,tableName,delete);
    }

    //删除列
    public static boolean deleteRow(Connection connection, String tableName, Delete delete) {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
            throw new HosServerException(ErrorCodes.ERROR_HBASE, "delete Qualifier error");
        }
        return true;
    }
    //删除行
    public static boolean deleRow(Connection connection, String tableName, String rowkey) {
        Delete delete = new Delete(rowkey.getBytes());
        return deleteRow(connection,tableName,delete);
    }
    //读取行
    public static Result getRow(Connection connection, String tableName, Get get){
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            return table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
            throw new HosServerException(ErrorCodes.ERROR_HBASE, "get data error");
        }
    }
    public static Result getRow(Connection connection, String tableName, String rowkey){
        Get get =new Get(rowkey.getBytes());
        return getRow(connection,tableName,get);
    }
    //获取scanner
    public static ResultScanner getScananer(Connection connection, String tableName, Scan scan){
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
            throw new HosServerException(ErrorCodes.ERROR_HBASE, "get scan error");
        }
    }
    //获取scanner
    public static ResultScanner getScananer(Connection connection, String tableName, String startKey, String endKey, FilterList filterList){
        Scan scan = new Scan();
        scan.setStartRow(startKey.getBytes());
        scan.setStopRow(endKey.getBytes());
        scan.setFilter(filterList);
        scan.setCaching(1000);
        return getScananer(connection,tableName,scan);
    }
    //插入行
    public static boolean putRow(Connection connection,String tableName,Put put){
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
            throw new HosServerException(ErrorCodes.ERROR_HBASE, "put row error");
        }
        return true;
    }
    public static boolean putRow(Connection connection, String tableName, String row, String columnFamily, String qualifier, String data){
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(data));
        putRow(connection, tableName,put);
        return true;
    }
    //批量插入
    public static boolean putRowList(Connection connection, String tableName, List<Put> puts){
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
            throw new HosServerException(ErrorCodes.ERROR_HBASE, "put rows error");
        }
        return true;
    }

    //incrementcolumnvalue  通过这个方法生成目录的seqid
    public static long incrementColumnValue(Connection connection,String tableName,String row,String cf,String qual,int num){
        try (Table table = connection.getTable(TableName.valueOf(tableName))){
            return table.incrementColumnValue(row.getBytes(),cf.getBytes(),qual.getBytes(),num);
        } catch (IOException e) {
            e.printStackTrace();
            throw new HosServerException(ErrorCodes.ERROR_HBASE, "put rows error");
        }
    }
}
