package com.yugabyte.crud.domain.exception;

/**
 * ビジネス例外基底クラス
 * すべてのビジネス例外の基底クラス
 */
public class BusinessException extends RuntimeException {
    
    private final ErrorCode errorCode;
    private final String errorName;
    private final String errorDetail;
    
    public BusinessException(ErrorCode errorCode, String errorName, String errorDetail) {
        this(errorCode, errorName, errorDetail, null);
    }
    
    public BusinessException(ErrorCode errorCode, String errorName, String errorDetail, Throwable cause) {
        super(String.format("[%s] %s: %s", errorCode.getCode(), errorName, errorDetail), cause);
        this.errorCode = errorCode;
        this.errorName = errorName;
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
    
    /**
     * エラーレスポンスDTOに変換
     */
    public com.yugabyte.crud.api.dto.ErrorResponse toErrorResponse() {
        return new com.yugabyte.crud.api.dto.ErrorResponse(
            errorCode.getCode(),
            errorName,
            errorDetail
        );
    }
}
