package com.wzl.bigtable.hos.core.test;

import com.wzl.bigtable.hos.core.usermgr.IUserService;
import com.wzl.bigtable.hos.core.usermgr.model.SystemRole;
import com.wzl.bigtable.hos.core.usermgr.model.UserInfo;
import com.wzl.bigtable.hos.mybatis.test.BaseTest;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Created by
 */
public class UserServiceTest extends BaseTest {

  @Autowired
  @Qualifier("userServiceImpl")
  IUserService userService;

  @Test
  public void addUser() {
    UserInfo userInfo = new UserInfo("wsj", "123456", SystemRole.ADMIN, "no desc");
    userService.addUser(userInfo);
  }

  @Test
  public void getUser() {
    UserInfo userInfo = userService.getUserInfoByName("wzl");
    System.out.println(userInfo.getUserId() + "|" + userInfo.getUserName() + "|" + userInfo.getPassword());
  }

  @Test
  public void deleteUser() {
    UserInfo userInfo = userService.getUserInfoByName("wsj");
    userService.deleteUser(userInfo.getUserId());
  }
}
