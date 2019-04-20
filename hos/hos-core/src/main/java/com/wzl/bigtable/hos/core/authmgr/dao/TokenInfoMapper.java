package com.wzl.bigtable.hos.core.authmgr.dao;


import com.wzl.bigtable.hos.core.authmgr.model.TokenInfo;
import org.apache.ibatis.annotations.*;

import java.util.Date;
import java.util.List;

/**
 * Created by wzl
 */
@Mapper
public interface TokenInfoMapper {

  @Insert("insert into TOKEN_INFO\n" +
          "    (TOKEN,EXPIRE_TIME,REFRESH_TIME,ACTIVE,CREATOR,CREATE_TIME)\n" +
          "    values\n" +
          "    (#{token.token},#{token.expireTime},#{token.refreshTime}\n" +
          "    ,#{token.active},#{token.creator},#{token.createTime})")
  public void addToken(@Param("token") TokenInfo tokenInfo);

  @Update("update TOKEN_INFO set EXPIRE_TIME=#{expireTime},ACTIVE=#{isActive}\n" +
          "    where TOKEN=#{token}")
  public void updateToken(@Param("token") String token, @Param("expireTime") int expireTime,
                          @Param("isActive") int isActive);

  @Update("update TOKEN_INFO set REFRESH_TIME=#{refreshTime}\n" +
          "    where TOKEN=#{token}")
  public void refreshToken(@Param("token") String token, @Param("refreshTime") Date refreshTime);

  @Delete("delete from TOKEN_INFO where TOKEN=#{token}")
  public void deleteToken(@Param("token") String token);

  @Select("select * from TOKEN_INFO where TOKEN=#{token}")
  @ResultMap("TokenInfoResultMap")
  public TokenInfo getTokenInfo(@Param("token") String token);

  @Select("select * from TOKEN_INFO where\n" +
          "    CREATOR=#{creator}")
  @ResultMap("TokenInfoResultMap")
  public List<TokenInfo> getTokenInfoList(@Param("creator") String creator);
}
