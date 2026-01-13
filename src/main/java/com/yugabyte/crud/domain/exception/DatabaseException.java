package com.yugabyte.crud.domain.exception;

/**
 * データベース例外クラス
 * データベース関連の例外を表現
 */
public class DatabaseException extends BusinessException {
    
    private final boolean timeout;
    
    public DatabaseException(ErrorCode errorCode, String errorName, String errorDetail) {
        this(errorCode, errorName, errorDetail, false, null);
    }
    
    public DatabaseException(ErrorCode errorCode, String errorName, String errorDetail, 
                           boolean timeout, Throwable cause) {
        super(errorCode, errorName, errorDetail, cause);
        this.timeout = timeout;
    }
    
    public boolean isTimeout() {
        return timeout;
    }
    
    // ファクトリメソッド
    
    public static DatabaseException tableNotFound(String tableName) {
        return new DatabaseException(
            ErrorCode.TABLE_NOT_FOUND,
            "table",
            String.format("テーブル '%s' は存在しません", tableName)
        );
    }
    
    public static DatabaseException connectionFailed(String message, Throwable cause) {
        return new DatabaseException(
            ErrorCode.CONNECTION_FAILED,
            "database_connection",
            "データベース接続に失敗しました: " + message,
            false,
            cause
        );
    }
    
    public static DatabaseException queryFailed(String operation, String message) {
        return new DatabaseException(
            ErrorCode.QUERY_FAILED,
            "sql_query",
            String.format("%s クエリの実行に失敗しました: %s", operation, message)
        );
    }
    
    public static DatabaseException queryFailed(String operation, String message, Throwable cause) {
        return new DatabaseException(
            ErrorCode.QUERY_FAILED,
            "sql_query",
            String.format("%s クエリの実行に失敗しました: %s", operation, message),
            false,
            cause
        );
    }
    
    public static DatabaseException noPrimaryKey(String tableName) {
        return new DatabaseException(
            ErrorCode.NO_PRIMARY_KEY,
            "primary_key",
            String.format("テーブル '%s' に主キーが定義されていません", tableName)
        );
    }
    
    public static DatabaseException configMissing(String message) {
        return new DatabaseException(
            ErrorCode.CONFIG_MISSING,
            "configuration",
            message
        );
    }
    
    public static DatabaseException timeout(String operation, String message, Throwable cause) {
        return new DatabaseException(
            ErrorCode.GATEWAY_TIMEOUT,
            "timeout",
            String.format("%s 操作がタイムアウトしました: %s", operation, message),
            true,
            cause
        );
    }
}
