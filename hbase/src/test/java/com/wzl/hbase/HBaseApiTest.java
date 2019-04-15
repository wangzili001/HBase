package com.wzl.hbase;

import org.junit.Test;

import static org.junit.Assert.*;

public class HBaseApiTest {

    @Test
    public void createTable() {
        Boolean table = new HBaseApi().createTable("FileTables", "fileInfo", "saveInfo");
        System.out.println(table);
    }
    @Test
    public void deleteTable(){
        Boolean fileTables = new HBaseApi().deleteTable("FileTables");
        System.out.println(fileTables);
    }
    @Test
    public void putData(){
        HBaseApi hBaseApi = new HBaseApi();
        Boolean aBoolean = hBaseApi.putData("student", "1004", "info", "name", "zhaoliu");
        Boolean aBoolean1 = hBaseApi.putData("student", "1004", "info", "sex", "male");
        System.out.println(aBoolean&aBoolean1);
    }
    @Test
    public void deleteData(){
        boolean student = new HBaseApi().deleteData("student", "1003");
        System.out.println(student);
    }
    @Test
    public void scanTable(){
        new HBaseApi().scanTable("student");
    }
    @Test
    public void getData(){
        new HBaseApi().getData("student","1001","info","name");
    }
}