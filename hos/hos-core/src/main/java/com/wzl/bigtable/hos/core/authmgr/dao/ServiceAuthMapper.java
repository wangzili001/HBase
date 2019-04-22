package com.wzl.bigtable.hos.core.authmgr.dao;



import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.wzl.bigtable.hos.core.authmgr.model.ServiceAuth;
import org.apache.ibatis.annotations.*;

/**
 * Created by wzl
 */
@Mapper
public interface ServiceAuthMapper extends BaseMapper<ServiceAuth> {
  @Insert("insert into SERVICE_AUTH\n" +
          "    (BucketName,TargetToken,AuthTime)\n" +
          "    values\n" +
          "    (#{auth.bucketName},#{auth.targetToken},#{auth.authTime})")
  public void addAuth(@Param("auth") ServiceAuth auth);

  @Delete("delete from SERVICE_AUTH\n" +
          "    where BucketName=#{bucket} AND TargetToken=#{token}")
  public void deleteAuth(@Param("bucket") String bucketName, @Param("token") String token);

  @Delete("delete from SERVICE_AUTH\n" +
          "    where TargetToken=#{token}")
  public void deleteAuthByToken(@Param("token") String token);

  @Delete("delete from SERVICE_AUTH\n" +
          "    where BucketName=#{bucket}")
  public void deleteAuthByBucket(@Param("bucket") String bucketName);

  @Select(" select * from SERVICE_AUTH where\n" +
          "    TargetToken=#{token} AND BucketName=#{bucket}")
  public ServiceAuth getAuth(@Param("bucket") String bucketName, @Param("token") String token);
}
