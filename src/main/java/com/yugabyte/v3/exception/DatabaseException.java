package com.yugabyte.v3.exception;

/**
 * データベース操作例外
 * データベース接続、クエリ、トランザクションなどのエラー処理に使用
 */
public class DatabaseException extends BusinessException {
    
    public DatabaseException(ErrorCode errorCode, String errorName, String errorDetail) {
        super(errorCode, errorName, errorDetail);
    }
    
    public DatabaseException(ErrorCode errorCode, String errorName, String errorDetail, Throwable cause) {
        super(errorCode, errorName, errorDetail, cause);
    }
    
    // 一般的なデータベースエラーのファクトリメソッド
    public static DatabaseException tableNotFound(String tableName) {
        return new DatabaseException(
            ErrorCode.TABLE_NOT_FOUND,
            tableName,
            String.format("テーブル '%s' は存在しません", tableName)
        );
    }
    
    public static DatabaseException connectionFailed(String reason) {
        return new DatabaseException(
            ErrorCode.CONNECTION_FAILED,
            "database_connection",
            String.format("データベース接続に失敗しました: %s", reason)
        );
    }
    
    public static DatabaseException queryFailed(String query, String reason) {
        return new DatabaseException(
            ErrorCode.QUERY_FAILED,
            "sql_query",
            String.format("SQLクエリの実行に失敗しました: %s, 理由: %s", query, reason)
        );
    }
    
    public static DatabaseException queryFailed(String query, String reason, Throwable cause) {
        return new DatabaseException(
            ErrorCode.QUERY_FAILED,
            "sql_query",
            String.format("SQLクエリの実行に失敗しました: %s, 理由: %s", query, reason),
            cause
        );
    }
    
    public static DatabaseException noPrimaryKey(String tableName) {
        return new DatabaseException(
            ErrorCode.NO_PRIMARY_KEY,
            tableName,
            String.format("テーブル '%s' に主キーが定義されていません", tableName)
        );
    }
    
    public static DatabaseException connectionFailed(String reason, Throwable cause) {
        return new DatabaseException(
            ErrorCode.CONNECTION_FAILED,
            "database_connection",
            String.format("データベース接続に失敗しました: %s", reason),
            cause
        );
    }

    // public static DatabaseException socketTimeout(String reason, Throwable cause) {
    //     return new DatabaseException(
    //         ErrorCode.GATEWAY_TIMEOUT,
    //         "database_timeout",
    //         String.format("データベース接続に失敗しました: %s", reason),
    //         cause
    //     );
    // }
    
    /**
     * 检查是否为超时异常
     */
    public boolean isTimeout() {
        Throwable cause = this.getCause();
        if (cause instanceof com.yugabyte.util.PSQLException) {
                // 检查根本原因是不是 SocketTimeoutException
            if (cause.getCause() instanceof java.net.SocketTimeoutException) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * 检查是否为连接异常
     */
    public boolean isConnectionError() {
        return this.getErrorCode() == ErrorCode.CONNECTION_FAILED || 
               this.getErrorCode() == ErrorCode.GATEWAY_TIMEOUT;
    }
}
