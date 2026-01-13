package com.yugabyte.v2.exception;

/**
 * 数据验证异常
 * 用于处理字段验证、数据格式等错误
 */
public class ValidationException extends BusinessException {
    
    public ValidationException(ErrorCode errorCode, String errorName, String errorDetail) {
        super(errorCode, errorName, errorDetail);
    }
    
    public ValidationException(ErrorCode errorCode, String errorName, String errorDetail, Throwable cause) {
        super(errorCode, errorName, errorDetail, cause);
    }
    
    // 常用验证错误的工厂方法
    public static ValidationException notNull(String fieldName) {
        return new ValidationException(
            ErrorCode.NOT_NULL, 
            fieldName, 
            String.format("字段 '%s' 不允许为空", fieldName)
        );
    }
    
    public static ValidationException invalidFormat(String fieldName, String expectedFormat) {
        return new ValidationException(
            ErrorCode.INVALID_FORMAT,
            fieldName,
            String.format("字段 '%s' 格式无效，期望格式: %s", fieldName, expectedFormat)
        );
    }
    
    public static ValidationException missingRequiredField(String fieldName) {
        return new ValidationException(
            ErrorCode.MISSING_REQUIRED_FIELD,
            fieldName,
            String.format("缺少必填字段: %s", fieldName)
        );
    }
    
    public static ValidationException unknownColumn(String columnName, String tableName) {
        return new ValidationException(
            ErrorCode.UNKNOWN_COLUMN,
            columnName,
            String.format("列 '%s' 在表 '%s' 中不存在", columnName, tableName)
        );
    }
    
    public static ValidationException invalidDataType(String columnName, String expectedType, String tableName) {
        return new ValidationException(
            ErrorCode.INVALID_FORMAT,
            columnName,
            String.format("列 '%s' 的数据类型无效，期望类型: %s，表: %s", columnName, expectedType, tableName)
        );
    }
}
