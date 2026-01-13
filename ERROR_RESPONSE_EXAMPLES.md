# 错误响应格式示例

新的V2版本实现了标准化的错误响应格式，包含以下字段：
- `errorCode`: 错误代码（如NOT_NULL、TABLE_NOT_FOUND等）
- `errorName`: 错误名称/字段名（如"apikey"、"request_body"等）
- `errorDetail`: 错误详情描述

## 响应结构

所有错误响应都遵循以下JSON结构：

```json
{
  "success": false,
  "error": {
    "errorCode": "ERROR_CODE",
    "errorName": "field_or_entity_name",
    "errorDetail": "详细错误描述"
  },
  "timestamp": 1234567890
}
```

## 成功响应结构

成功响应结构：

```json
{
  "success": true,
  "data": { ... },
  "timestamp": 1234567890
}
```

## 错误示例

### 1. 字段不允许为空（NOT_NULL）
```json
{
  "success": false,
  "error": {
    "errorCode": "NOT_NULL",
    "errorName": "apikey",
    "errorDetail": "字段 'apikey' 不允许为空"
  },
  "timestamp": 1735027200000
}
```

### 2. 表不存在（TABLE_NOT_FOUND）
```json
{
  "success": false,
  "error": {
    "errorCode": "TABLE_NOT_FOUND",
    "errorName": "users",
    "errorDetail": "表 'users' 不存在"
  },
  "timestamp": 1735027200000
}
```

### 3. 缺少请求体（MISSING_BODY）
```json
{
  "success": false,
  "error": {
    "errorCode": "MISSING_BODY",
    "errorName": "request_body",
    "errorDetail": "请求体不能为空"
  },
  "timestamp": 1735027200000
}
```

### 4. 缺少必填字段（MISSING_REQUIRED_FIELDS）
```json
{
  "success": false,
  "error": {
    "errorCode": "MISSING_REQUIRED_FIELDS",
    "errorName": "required_fields",
    "errorDetail": "缺少必填字段: username, email"
  },
  "timestamp": 1735027200000
}
```

### 5. 数据库配置缺失（DB_CONFIG_MISSING）
```json
{
  "success": false,
  "error": {
    "errorCode": "DB_CONFIG_MISSING",
    "errorName": "environment_variables",
    "errorDetail": "数据库连接配置缺失，请检查DB_URL、DB_USER、DB_PASSWORD环境变量"
  },
  "timestamp": 1735027200000
}
```

### 6. 不支持的HTTP方法（METHOD_NOT_SUPPORTED）
```json
{
  "success": false,
  "error": {
    "errorCode": "METHOD_NOT_SUPPORTED",
    "errorName": "http_method",
    "errorDetail": "不支持的HTTP方法: PUT"
  },
  "timestamp": 1735027200000
}
```

### 7. 数据库操作失败（DATABASE_ERROR）
```json
{
  "success": false,
  "error": {
    "errorCode": "DATABASE_ERROR",
    "errorName": "database_operation",
    "errorDetail": "数据库操作失败: Connection refused"
  },
  "timestamp": 1735027200000
}
```

### 8. 系统内部错误（INTERNAL_ERROR）
```json
{
  "success": false,
  "error": {
    "errorCode": "INTERNAL_ERROR",
    "errorName": "system",
    "errorDetail": "系统内部错误，请联系管理员"
  },
  "timestamp": 1735027200000
}
```

## 使用方式

新的V2 API通过以下路由访问：
- 原始API: `/{table}`
- V2 API: `/v2/{table}`

两个版本可以共存，V2版本提供了更详细的错误信息和标准化的响应格式。

## 代码结构

新的V2实现位于 `src/main/java/com/yugabyte/v2/` 目录下：
- `dto/ErrorResponse.java` - 错误响应DTO
- `exception/` - 自定义异常类
- `handler/CrudHandlerV2.java` - 增强的CRUD处理器
- `service/DatabaseMetadataServiceV2.java` - 增强的元数据服务
- `function/FunctionV2.java` - 主函数入口
- `config/ResponseUtil.java` - 响应工具类

## 优势

1. **标准化错误响应**: 所有错误都遵循相同的JSON结构
2. **详细错误信息**: 包含errorCode、errorName、errorDetail三个字段
3. **更好的可读性**: 错误信息更清晰，便于调试
4. **工程化结构**: 代码分层清晰，易于维护和扩展
5. **向后兼容**: 原始API保持不变，V2 API提供增强功能
