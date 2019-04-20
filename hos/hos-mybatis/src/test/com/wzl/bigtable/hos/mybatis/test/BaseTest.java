package com.wzl.bigtable.hos.mybatis.test;

import org.junit.runner.RunWith;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@PropertySource("classpath:application.properties")
@MapperScan("com.wzl.bigtable.hos.**")
@ComponentScan("com.wzl.bigtable.hos.**")
public class BaseTest {
}
