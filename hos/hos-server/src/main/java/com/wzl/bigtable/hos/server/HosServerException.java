package com.wzl.bigtable.hos.server;


import com.wzl.bigtable.hos.core.HosException;

/**
 * HosServer模块异常.
 */
public class HosServerException extends HosException {

  private int code;
  private String message;

  public HosServerException(int code, String message, Throwable cause) {
    super(message, cause);
    this.code = code;
    this.message = message;
  }

  public HosServerException(int code, String message) {
    super(message, null);
    this.code = code;
    this.message = message;
  }

  public int getCode() {
    return code;
  }

  @Override
  public String getMessage() {
    return message;
  }

  @Override
  public int errorCode() {
    return this.code;
  }
}
