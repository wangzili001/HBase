package com.wzl.bigtable.hos.server.dao;

import com.wzl.bigtable.common.BucketModel;
import org.apache.ibatis.annotations.*;

import java.util.List;

/**
 * Created by wzl
 */
@Mapper
public interface BucketMapper {

  @Insert("insert into HOS_BUCKET\n" +
          "    (BUCKET_ID,BUCKET_NAME,CREATOR,DETAIL,CREATE_TIME)\n" +
          "    values\n" +
          "    (#{bucket.bucketId},#{bucket.bucketName},#{bucket.creator}\n" +
          "    ,#{bucket.detail},#{bucket.createTime})")
  void addBucket(@Param("bucket") BucketModel bucketModel);

  @Update("update HOS_BUCKET set BUCKET_NAME=#{bucketName}\n" +
          "    <if test=\"detail!=null and detail!='' \">\n" +
          "      , DETAIL=#{detail}\n" +
          "    </if>\n" +
          "    where BUCKET_NAME=#{bucketName}")
  int updateBucket(@Param("bucketName") String bucketName, @Param("detail") String detail);

  @Delete("delete from HOS_BUCKET where BUCKET_NAME=#{bucketName}")
  int deleteBucket(@Param("bucketName") String bucketName);

  @Select("select * from HOS_BUCKET where\n" +
          "    BUCKET_ID=#{bucketId}")
  @ResultMap("BucketResultMap")
  BucketModel getBucket(@Param("bucketId") String bucketId);

  @Select(" select * from HOS_BUCKET where\n" +
          "    BUCKET_NAME=#{bucketName}")
  @ResultMap("BucketResultMap")
  BucketModel getBucketByName(@Param("bucketName") String bucketName);

  @Select(" select * from HOS_BUCKET where\n" +
          "    CREATOR=#{creator}")
  @ResultMap("BucketResultMap")
  List<BucketModel> getBucketByCreator(@Param("creator") String creator);

  @Select(" select b.* from HOS_BUCKET b,SERVICE_AUTH s where\n" +
          "    s.TARGET_TOKEN=#{token} and s.BUCKET_NAME=b.BUCKET_NAME")
  @ResultMap("BucketResultMap")
  List<BucketModel> getUserAuthorizedBuckets(@Param("token") String token);
}
