package com.wzl.hbase.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PhoenixTest {
    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");

        Connection connection = DriverManager.getConnection("jdbc:phoenix:master:2181");

        PreparedStatement statement = connection.prepareStatement("select * from person");

        ResultSet resultSet = statement.executeQuery();

        while (resultSet.next()){
            System.out.println(resultSet.getString("NAME"));
        }
        statement.close();
        connection.close();
    }
}
