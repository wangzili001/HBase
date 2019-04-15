package com.wzl.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class HBaseConn {

    private static final HBaseConn INSTANCE = new HBaseConn();
    //conf
    private static Configuration conf;
    //connection
    private static Connection connection;

    public HBaseConn() {
        if(conf==null){
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum","master:2181");
        }
    }

    public static Table getTable(String tableName) throws IOException {
        HBaseAdmin admin = (HBaseAdmin)getHBaseConn().getAdmin();
        if(!admin.tableExists(tableName)){
            throw new IOException("表不存在");
        }
        return INSTANCE.getConnection().getTable(TableName.valueOf(tableName));
    }

    private Connection getConnection() throws IOException {
        if(connection==null||connection.isClosed()){
            connection = ConnectionFactory.createConnection(conf);
        }
        return connection;
    }
    public static Connection getHBaseConn() throws IOException {
        return INSTANCE.getConnection();
    }

    public static void closeConn(){
        if(connection!=null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
