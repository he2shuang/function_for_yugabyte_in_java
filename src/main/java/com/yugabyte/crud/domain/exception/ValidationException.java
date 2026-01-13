package com.yugabyte.crud.domain.exception;

/**
 * 検証例外クラス
 * 入力検証関連の例外を表現
 */
public class ValidationException extends BusinessException {
    
    public ValidationException(ErrorCode errorCode, String errorName, String errorDetail) {
        super(errorCode, errorName, errorDetail);
    }
    
    public ValidationException(ErrorCode errorCode, String errorName, String errorDetail, Throwable cause) {
        super(errorCode, errorName, errorDetail, cause);
    }
    
    // ファクトリメソッド
    
    public static ValidationException unknownColumn(String columnName, String tableName) {
        return new ValidationException(
            ErrorCode.UNKNOWN_COLUMN,
            "column",
            String.format("列 '%s' はテーブル '%s' に存在しません", columnName, tableName)
        );
    }
    
    public static ValidationException missingRequiredField(String fieldName) {
        return new ValidationException(
            ErrorCode.MISSING_REQUIRED_FIELD,
            "required_field",
            String.format("必須フィールド '%s' が不足しています", fieldName)
        );
    }
    
    public static ValidationException notNull(String fieldName) {
        return new ValidationException(
            ErrorCode.NOT_NULL,
            "not_null",
            String.format("フィールド '%s' は空にできません", fieldName)
        );
    }
    
    public static ValidationException invalidFormat(String fieldName, String reason) {
        return new ValidationException(
            ErrorCode.INVALID_FORMAT,
            "format",
            String.format("フィールド '%s' の形式が無効です: %s", fieldName, reason)
        );
    }
    
    public static ValidationException missingBody() {
        return new ValidationException(
            ErrorCode.MISSING_BODY,
            "request_body",
            "リクエストボディは空にできません"
        );
    }
    
    public static ValidationException missingFilter() {
        return new ValidationException(
            ErrorCode.MISSING_FILTER,
            "query_parameters",
            "フィルタリングのためのクエリパラメータが必要です"
        );
    }
    
    public static ValidationException noValidColumns() {
        return new ValidationException(
            ErrorCode.NO_VALID_COLUMNS,
            "request_body",
            "リクエストボディに更新する有効な列が提供されていません"
        );
    }
    
    public static ValidationException noValidFilters() {
        return new ValidationException(
            ErrorCode.NO_VALID_FILTERS,
            "query_parameters",
            "フィルタリングのための有効なクエリパラメータが提供されていません"
        );
    }
    
    public static ValidationException invalidTableName(String reason) {
        return new ValidationException(
            ErrorCode.INVALID_TABLE_NAME,
            "table_name",
            reason
        );
    }
    
    public static ValidationException invalidHttpMethod(String reason) {
        return new ValidationException(
            ErrorCode.INVALID_HTTP_METHOD,
            "http_method",
            reason
        );
    }
}
