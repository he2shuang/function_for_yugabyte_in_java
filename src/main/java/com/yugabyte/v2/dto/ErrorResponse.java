package com.yugabyte.v2.dto;

import com.google.gson.annotations.SerializedName;

/**
 * 标准错误响应DTO
 * 包含errorCode、errorName、errorDetail三个字段
 */
public class ErrorResponse {
    
    @SerializedName("errorCode")
    private final String errorCode;
    
    @SerializedName("errorName")
    private final String errorName;
    
    @SerializedName("errorDetail")
    private final String errorDetail;
    
    public ErrorResponse(String errorCode, String errorName, String errorDetail) {
        this.errorCode = errorCode;
        this.errorName = errorName;
        this.errorDetail = errorDetail;
    }
    
    public String getErrorCode() {
        return errorCode;
    }
    
    public String getErrorName() {
        return errorName;
    }
    
    public String getErrorDetail() {
        return errorDetail;
    }
    
    @Override
    public String toString() {
        return "ErrorResponse{" +
                "errorCode='" + errorCode + '\'' +
                ", errorName='" + errorName + '\'' +
                ", errorDetail='" + errorDetail + '\'' +
                '}';
    }
}
