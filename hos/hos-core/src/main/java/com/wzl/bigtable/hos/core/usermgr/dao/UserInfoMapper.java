package com.wzl.bigtable.hos.core.usermgr.dao;

import com.wzl.bigtable.hos.core.usermgr.model.UserInfo;
import org.apache.ibatis.annotations.*;

/**
 * Created by wzl
 */
@Mapper
public interface UserInfoMapper {

  @Insert("insert into USER_INFO\n" +
          "    (USER_ID,USER_NAME,PASSWORD,SYSTEM_ROLE,DETAIL,CREATE_TIME)\n" +
          "    values\n" +
          "    (#{userInfo.userId},#{userInfo.userName},#{userInfo.password}\n" +
          "    ,#{userInfo.systemRole},#{userInfo.detail},#{userInfo.createTime})")
  void addUser(@Param("userInfo") UserInfo userInfo);

  @Update("update USER_INFO set USER_ID=#{userId}\n" +
          "    <if test=\"password!=null and password!='' \">\n" +
          "      , PASSWORD=#{password}\n" +
          "    </if>\n" +
          "    <if test=\"detail!=null and detail!='' \">\n" +
          "      , DETAIL=#{detail}\n" +
          "    </if>\n" +
          "    where USER_ID=#{userId}")
  int updateUserInfo(@Param("userId") String userId, @Param("password") String password, @Param("detail") String detail);

  @Delete("delete from USER_INFO where USER_ID=#{userId}")
  int deleteUser(@Param("userId") String userId);

  @Select("select * from USER_INFO where\n" +
          "    USER_ID=#{userId}")
  @ResultMap("UserInfoResultMap")
  UserInfo getUserInfo(@Param("userId") String userId);

  @Select("select * from USER_INFO\n" +
          "    where PASSWORD=#{password} and USER_NAME=#{userName}")
  UserInfo checkPassword(@Param("userName") String userName, @Param("password") String password);

  @Select("select * from USER_INFO where\n" +
          "    USER_NAME=#{userName}")
  @ResultMap("UserInfoResultMap")
  UserInfo getUserInfoByName(@Param("userName") String userName);
}
