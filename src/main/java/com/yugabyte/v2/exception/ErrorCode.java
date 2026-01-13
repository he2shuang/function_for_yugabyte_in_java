package com.yugabyte.v2.exception;

/**
 * 错误代码枚举
 * 集中管理所有错误代码
 */
public enum ErrorCode {
    
    // 验证错误 (400)
    NOT_NULL("NotNull", "字段不允许为空"),
    INVALID_FORMAT("InvalidFormat", "格式无效"),
    MISSING_REQUIRED_FIELD("MissingRequiredField", "缺少必填字段"),
    MISSING_BODY("MissingBody", "请求体不能为空"),
    MISSING_FILTER("MissingFilter", "缺少过滤条件"),
    NO_VALID_COLUMNS("NoValidColumns", "未提供有效的列"),
    NO_VALID_FILTERS("NoValidFilters", "未提供有效的过滤条件"),
    METHOD_NOT_SUPPORTED("MethodNotSupported", "不支持的HTTP方法"),
    UNKNOWN_COLUMN("UnknownColumn", "未知的列"),
    
    // 数据库错误 (404/500)
    TABLE_NOT_FOUND("TableNotFound", "表不存在"),
    CONNECTION_FAILED("ConnectionFailed", "数据库连接失败"),
    QUERY_FAILED("QueryFailed", "SQL查询执行失败"),
    NO_PRIMARY_KEY("NoPrimaryKey", "表没有定义主键"),
    DATABASE_ERROR("DatabaseError", "数据库操作失败"),
    
    // 配置错误 (500)
    DB_CONFIG_MISSING("DbConfigMissing", "数据库连接配置缺失"),
    
    // 系统错误 (500)
    INTERNAL_ERROR("InternalError", "系统内部错误");
    
    private final String code;
    private final String description;
    
    ErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }
    
    public String getCode() {
        return code;
    }
    
    public String getDescription() {
        return description;
    }
    
    @Override
    public String toString() {
        return code;
    }
}
