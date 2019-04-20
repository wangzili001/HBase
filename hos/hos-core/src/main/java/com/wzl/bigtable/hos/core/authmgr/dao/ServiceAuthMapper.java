package com.wzl.bigtable.hos.core.authmgr.dao;



import com.wzl.bigtable.hos.core.authmgr.model.ServiceAuth;
import org.apache.ibatis.annotations.*;

/**
 * Created by wzl
 */
@Mapper
public interface ServiceAuthMapper {
  @Insert("insert into SERVICE_AUTH\n" +
          "    (BUCKET_NAME,TARGET_TOKEN,AUTH_TIME)\n" +
          "    values\n" +
          "    (#{auth.bucketName},#{auth.targetToken},#{auth.authTime})")
  public void addAuth(@Param("auth") ServiceAuth auth);

  @Delete("delete from SERVICE_AUTH\n" +
          "    where BUCKET_NAME=#{bucket} AND TARGET_TOKEN=#{token}")
  public void deleteAuth(@Param("bucket") String bucketName, @Param("token") String token);

  @Delete("delete from SERVICE_AUTH\n" +
          "    where TARGET_TOKEN=#{token}")
  public void deleteAuthByToken(@Param("token") String token);

  @Delete("delete from SERVICE_AUTH\n" +
          "    where BUCKET_NAME=#{bucket}")
  public void deleteAuthByBucket(@Param("bucket") String bucketName);

  @Select(" select * from SERVICE_AUTH where\n" +
          "    TARGET_TOKEN=#{token} AND BUCKET_NAME=#{bucket}")
  @ResultMap("ServiceAuthResultMap")
  public ServiceAuth getAuth(@Param("bucket") String bucketName, @Param("token") String token);
}
