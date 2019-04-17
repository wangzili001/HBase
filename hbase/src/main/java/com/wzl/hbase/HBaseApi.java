package com.wzl.hbase;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * 操作Api
 */
public class HBaseApi {
    /**
     * 创建表
     * @param tableName 表名
     * @param cfs 列族
     * @return isSuccess
     */
    public Boolean createTable(String tableName,String... cfs){
        try (HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()){
            if(admin.tableExists(tableName)){
                throw new IOException("表已存在！");
            }
            ////创建表描述器
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            Arrays.asList(cfs).forEach(cf->{
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
                columnDescriptor.setMaxVersions(1);
                tableDescriptor.addFamily(columnDescriptor);
            });
            admin.createTable(tableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * hbase插入一条数据
     * @param tableName 表名
     * @param rowKey 唯一标识
     * @param cfName 列族名
     * @param qualifier 列标识
     * @param data 值
     * @return 是否插入成功
     */
    public Boolean putData(String tableName,String rowKey,String cfName,String qualifier,String data){
        try(Table table = HBaseConn.getTable(tableName)) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(cfName),Bytes.toBytes(qualifier),Bytes.toBytes(data));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
    /**
     * 批量插入
     * @param tableName
     * @param puts
     * @return
     */
    public static boolean putDatas(String tableName, List<Put> puts){
        try(Table table = HBaseConn.getTable(tableName)) {
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
    /**
     * 删除一行记录
     * @param tableName 表名
     * @param rowKey  唯一标识号
     * @return 是否成功
     */
    public static boolean deleteData(String tableName,String rowKey){
        try(Table table = HBaseConn.getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 删除 列名
     * @param tableName
     * @param rowKey
     * @param cfName
     * @param qualifier
     * @return
     */
    public static boolean deleteQualifier(String tableName,String rowKey,String cfName,String qualifier){
        try(Table table = HBaseConn.getTable(tableName)){
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            //带s删除所有版本
            delete.addColumns(Bytes.toBytes(cfName),Bytes.toBytes(qualifier));
            table.delete(delete);
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 根据过滤器筛选数据
     * @param tableName  表名
     * @param rowKey  列名
     * @param filterList 过滤规则
     * @return 查询结果
     */
    public static Result getRow(String tableName, String rowKey, FilterList filterList){
        try(Table table = HBaseConn.getTable(tableName)){
            Get get = new Get(Bytes.toBytes(rowKey));
            get.setFilter(filterList);
            return table.get(get);
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    /**
     * 全表扫描
     * @param tableName 表名
     * @return 扫描集合
     */
    public static void scanTable(String tableName){
        try(Table table = HBaseConn.getTable(tableName)) {
            //构建扫描器
            Scan scan = new Scan();
            ResultScanner results = table.getScanner(scan);
            //遍历数据并打印
            //1.对rowkey遍历
            results.forEach(result -> {
                Cell[] cells = result.rawCells();
                //2.对cell遍历
                for (Cell cell : cells) {
                    System.out.println("RK:"+Bytes.toString(CellUtil.cloneRow(cell))
                    +",CF:"+Bytes.toString(CellUtil.cloneFamily(cell))
                    +",CN"+Bytes.toString(CellUtil.cloneQualifier(cell))
                    +",Value:"+Bytes.toString(CellUtil.cloneValue(cell)));
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void getData(String tableName,String rowkey,String cf,String cn){
        try(Table table = HBaseConn.getTable(tableName)) {
            //获取get对象
            Get get = new Get(Bytes.toBytes(rowkey));
            //获取指定列族
            if(cf.length() != 0&cn.length() != 0){
                get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
            }
            get.setMaxVersions(1);
            //获取数据
            Result result = table.get(get);
            Cell[] cells = result.rawCells();
            //2.对cell遍历
            for (Cell cell : cells) {
                System.out.println("RK:"+Bytes.toString(CellUtil.cloneRow(cell))
                        +",CF:"+Bytes.toString(CellUtil.cloneFamily(cell))
                        +",CN"+Bytes.toString(CellUtil.cloneQualifier(cell))
                        +",Value:"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除表
     * @param taleName 表名
     * @return isSuccess
     */
    public Boolean deleteTable(String taleName){
        try(HBaseAdmin admin = (HBaseAdmin)HBaseConn.getHBaseConn().getAdmin()){
            if(admin.tableExists(taleName)){
                throw new IOException("表已存在");
            }
            //使表不可用
            admin.disableTable(taleName);
            admin.deleteTable(taleName);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

}
