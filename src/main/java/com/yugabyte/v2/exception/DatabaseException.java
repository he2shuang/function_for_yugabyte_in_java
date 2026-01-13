package com.yugabyte.v2.exception;

/**
 * 数据库操作异常
 * 用于处理数据库连接、查询、事务等错误
 */
public class DatabaseException extends BusinessException {
    
    public DatabaseException(ErrorCode errorCode, String errorName, String errorDetail) {
        super(errorCode, errorName, errorDetail);
    }
    
    public DatabaseException(ErrorCode errorCode, String errorName, String errorDetail, Throwable cause) {
        super(errorCode, errorName, errorDetail, cause);
    }
    
    // 常用数据库错误的工厂方法
    public static DatabaseException tableNotFound(String tableName) {
        return new DatabaseException(
            ErrorCode.TABLE_NOT_FOUND,
            tableName,
            String.format("表 '%s' 不存在", tableName)
        );
    }
    
    public static DatabaseException connectionFailed(String reason) {
        return new DatabaseException(
            ErrorCode.CONNECTION_FAILED,
            "database_connection",
            String.format("数据库连接失败: %s", reason)
        );
    }
    
    public static DatabaseException queryFailed(String query, String reason) {
        return new DatabaseException(
            ErrorCode.QUERY_FAILED,
            "sql_query",
            String.format("SQL查询执行失败: %s, 原因: %s", query, reason)
        );
    }
    
    public static DatabaseException noPrimaryKey(String tableName) {
        return new DatabaseException(
            ErrorCode.NO_PRIMARY_KEY,
            tableName,
            String.format("表 '%s' 没有定义主键", tableName)
        );
    }
}
