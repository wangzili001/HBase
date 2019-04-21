package com.wzl.bigtable.hos.server;



import com.wzl.bigtable.common.BucketModel;
import com.wzl.bigtable.hos.core.usermgr.model.UserInfo;

import java.util.List;

/**
 * Created by wzl
 */
public interface IBucketService {

  public boolean addBucket(UserInfo userInfo, String bucketName, String detail);

  public boolean deleteBucket(String bucketName);

  public boolean updateBucket(String bucketName, String detail);

  public BucketModel getBucketById(String bucketId);

  public BucketModel getBucketByName(String bucketName);

  public List<BucketModel> getUserBuckets(String token);
}
