package com.wzl.bigtable.hos.core.authmgr.dao;


import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.wzl.bigtable.hos.core.authmgr.model.TokenInfo;
import org.apache.ibatis.annotations.*;

import java.util.Date;
import java.util.List;

/**
 * Created by wzl
 */
@Mapper
public interface TokenInfoMapper extends BaseMapper<TokenInfo> {

  @Insert("insert into TOKEN_INFO\n" +
          "    (Token,ExpireTime,RefreshTime,Active,Creator,CreateTime)\n" +
          "    values\n" +
          "    (#{token.token},#{token.expireTime},#{token.refreshTime}\n" +
          "    ,#{token.active},#{token.creator},#{token.createTime})")
  public void addToken(@Param("token") TokenInfo tokenInfo);

  @Update("update TOKEN_INFO set ExpireTime=#{expireTime},Active=#{isActive}\n" +
          "    where TOKEN=#{token}")
  public void updateToken(@Param("token") String token, @Param("expireTime") int expireTime,
                          @Param("isActive") int isActive);

  @Update("update TOKEN_INFO set refreshTime=#{refreshTime}\n" +
          "    where TOKEN=#{token}")
  public void refreshToken(@Param("token") String token, @Param("refreshTime") Date refreshTime);

  @Delete("delete from TOKEN_INFO where TOKEN=#{token}")
  public void deleteToken(@Param("token") String token);

  @Select("select * from TOKEN_INFO where TOKEN=#{token}")
  public TokenInfo getTokenInfo(@Param("token") String token);

  @Select("select * from TOKEN_INFO where\n" +
          "    Creator=#{Creator}")
  public List<TokenInfo> getTokenInfoList(@Param("Creator") String Creator);
}
