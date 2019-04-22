package com.wzl.bigtable.hos.server.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.wzl.bigtable.common.BucketModel;
import org.apache.ibatis.annotations.*;

import java.util.List;

/**
 * Created by wzl
 */
@Mapper
public interface BucketMapper extends BaseMapper<BucketModel> {

  @Insert("insert into HOS_BUCKET\n" +
          "    (BucketId,BucketName,Creator,Detail,CreateTime)\n" +
          "    values\n" +
          "    (#{bucket.bucketId},#{bucket.bucketName},#{bucket.Creator}\n" +
          "    ,#{bucket.Detail},#{bucket.createTime})")
  void addBucket(@Param("bucket") BucketModel bucketModel);

  @Update("update HOS_BUCKET set BucketName=#{bucketName}\n" +
          "    <if test=\"Detail!=null and Detail!='' \">\n" +
          "      , Detail=#{Detail}\n" +
          "    </if>\n" +
          "    where BucketName=#{bucketName}")
  int updateBucket(@Param("bucketName") String bucketName, @Param("Detail") String Detail);

  @Delete("delete from HOS_BUCKET where BucketName=#{bucketName}")
  int deleteBucket(@Param("bucketName") String bucketName);

  @Select("select * from HOS_BUCKET where\n" +
          "    BucketId=#{bucketId}")
  BucketModel getBucket(@Param("bucketId") String bucketId);

  @Select(" select * from HOS_BUCKET where\n" +
          "    BucketName=#{bucketName}")
  BucketModel getBucketByName(@Param("bucketName") String bucketName);

  @Select(" select * from HOS_BUCKET where\n" +
          "    Creator=#{Creator}")
  List<BucketModel> getBucketByCreator(@Param("Creator") String Creator);

  @Select(" select b.* from HOS_BUCKET b,SERVICE_AUTH s where\n" +
          "    s.TargetToken=#{token} and s.BucketName=b.BucketName")
  List<BucketModel> getUserAuthorizedBuckets(@Param("token") String token);
}
