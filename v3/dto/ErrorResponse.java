package com.yugabyte.v3.dto;

import com.google.gson.annotations.SerializedName;

/**
 * 標準エラーレスポンスDTO
 * errorCode、errorName、errorDetailの3つのフィールドを含む
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
