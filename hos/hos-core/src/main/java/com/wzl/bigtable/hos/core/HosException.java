package com.wzl.bigtable.hos.core;

public abstract class HosException extends RuntimeException {
    protected String errorMessage;

    public HosException(String errorMessage,Throwable cause) {
        super(cause);
        this.errorMessage = errorMessage;
    }

    public abstract int errorCode();

    public String errorMessage(){
        return this.errorMessage;
    }
}
