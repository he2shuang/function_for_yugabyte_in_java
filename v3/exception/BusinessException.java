package com.yugabyte.v3.exception;

import com.yugabyte.v3.dto.ErrorResponse;

/**
 * ビジネス例外基底クラス
 * エラーコード、エラー名、エラー詳細を含む
 */
public class BusinessException extends RuntimeException {
    
    private final ErrorCode errorCode;
    private final String errorName;
    private final String errorDetail;
    
    public BusinessException(ErrorCode errorCode, String errorName, String errorDetail) {
        super(errorDetail);
        this.errorCode = errorCode;
        this.errorName = errorName != null ? errorName : "-";
        this.errorDetail = errorDetail;
    }
    
    public BusinessException(ErrorCode errorCode, String errorName, String errorDetail, Throwable cause) {
        super(errorDetail, cause);
        this.errorCode = errorCode;
        this.errorName = errorName != null ? errorName : "-";
        this.errorDetail = errorDetail;
    }
    
    public ErrorCode getErrorCode() {
        return errorCode;
    }
    
    public String getErrorName() {
        return errorName;
    }
    
    public String getErrorDetail() {
        return errorDetail;
    }
    
    public ErrorResponse toErrorResponse() {
        return new ErrorResponse(errorCode.getCode(), errorName, errorDetail);
    }
}
