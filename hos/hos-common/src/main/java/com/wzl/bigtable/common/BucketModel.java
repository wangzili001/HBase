package com.wzl.bigtable.common;



import com.wzl.bigtable.hos.core.usermgr.CoreUtil;

import java.util.Date;

/**
 * Created by wzl
 */
public class BucketModel {

  private String BucketId;
  private String BucketName;
  private String Creator;
  private String Detail;
  private Date CreateTime;

  public BucketModel(String BucketName, String Creator, String Detail) {
    this.BucketId = CoreUtil.getUUID();
    this.BucketName = BucketName;
    this.CreateTime = new Date();
    this.Creator = Creator;
    this.Detail = Detail;
  }

  public BucketModel() {

  }

  public String getBucketId() {
    return BucketId;
  }

  public void setBucketId(String BucketId) {
    this.BucketId = BucketId;
  }

  public String getBucketName() {
    return BucketName;
  }

  public void setBucketName(String BucketName) {
    this.BucketName = BucketName;
  }

  public String getCreator() {
    return Creator;
  }

  public void setCreator(String Creator) {
    this.Creator = Creator;
  }

  public String getDetail() {
    return Detail;
  }

  public void setDetail(String Detail) {
    this.Detail = Detail;
  }

  public Date getCreateTime() {
    return CreateTime;
  }

  public void setCreateTime(Date CreateTime) {
    this.CreateTime = CreateTime;
  }
}
