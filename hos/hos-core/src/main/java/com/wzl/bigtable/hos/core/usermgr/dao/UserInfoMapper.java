package com.wzl.bigtable.hos.core.usermgr.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.wzl.bigtable.hos.core.usermgr.model.UserInfo;
import org.apache.ibatis.annotations.*;

/**
 * Created by wzl
 */
@Mapper
public interface UserInfoMapper extends BaseMapper<UserInfo> {

  @Insert("insert into USER_INFO\n" +
          "    (UserId,UserName,Password,SystemRole,DETAIL,CreateTime)\n" +
          "    values\n" +
          "    (#{userInfo.userId},#{userInfo.userName},#{userInfo.password}\n" +
          "    ,#{userInfo.systemRole},#{userInfo.detail},#{userInfo.createTime})")
  void addUser(@Param("userInfo") UserInfo userInfo);

  @Update("update USER_INFO set UserId=#{userId}\n" +
          "    <if test=\"password!=null and password!='' \">\n" +
          "      , PASSWORD=#{password}\n" +
          "    </if>\n" +
          "    <if test=\"detail!=null and detail!='' \">\n" +
          "      , DETAIL=#{detail}\n" +
          "    </if>\n" +
          "    where UserId=#{userId}")
  int updateUserInfo(@Param("userId") String userId, @Param("password") String password, @Param("detail") String detail);

  @Delete("delete from USER_INFO where UserId=#{userId}")
  int deleteUser(@Param("userId") String userId);

  @Select("select * from USER_INFO where\n" +
          "    UserId=#{userId}")
  UserInfo getUserInfo(@Param("userId") String userId);

  @Select("select * from USER_INFO\n" +
          "    where PASSWORD=#{password} and UserName=#{userName}")
  UserInfo checkPassword(@Param("userName") String userName, @Param("password") String password);

  @Select("select * from USER_INFO where\n" +
          "    UserName=#{userName}")
  UserInfo getUserInfoByName(@Param("userName") String userName);
}
