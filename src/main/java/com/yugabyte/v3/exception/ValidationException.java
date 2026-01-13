package com.yugabyte.v3.exception;

/**
 * データ検証例外
 * フィールド検証、データ形式などのエラー処理に使用
 */
public class ValidationException extends BusinessException {
    
    public ValidationException(ErrorCode errorCode, String errorName, String errorDetail) {
        super(errorCode, errorName, errorDetail);
    }
    
    public ValidationException(ErrorCode errorCode, String errorName, String errorDetail, Throwable cause) {
        super(errorCode, errorName, errorDetail, cause);
    }
    
    // 一般的な検証エラーのファクトリメソッド
    public static ValidationException notNull(String fieldName) {
        return new ValidationException(
            ErrorCode.NOT_NULL, 
            fieldName, 
            String.format("フィールド '%s' は空にできません", fieldName)
        );
    }
    
    public static ValidationException invalidFormat(String fieldName, String expectedFormat) {
        return new ValidationException(
            ErrorCode.INVALID_FORMAT,
            fieldName,
            String.format("フィールド '%s' の形式が無効です。期待される形式: %s", fieldName, expectedFormat)
        );
    }
    
    public static ValidationException missingRequiredField(String fieldName) {
        return new ValidationException(
            ErrorCode.MISSING_REQUIRED_FIELD,
            fieldName,
            String.format("必須フィールドが不足しています: %s", fieldName)
        );
    }
    
    public static ValidationException unknownColumn(String columnName, String tableName) {
        return new ValidationException(
            ErrorCode.UNKNOWN_COLUMN,
            columnName,
            String.format("列 '%s' はテーブル '%s' に存在しません", columnName, tableName)
        );
    }
    
    public static ValidationException invalidDataType(String columnName, String expectedType, String tableName) {
        return new ValidationException(
            ErrorCode.INVALID_FORMAT,
            columnName,
            String.format("列 '%s' のデータ型が無効です。期待される型: %s, テーブル: %s", columnName, expectedType, tableName)
        );
    }
}
