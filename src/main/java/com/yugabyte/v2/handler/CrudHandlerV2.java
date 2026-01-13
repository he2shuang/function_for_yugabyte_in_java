package com.yugabyte.v2.handler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.yugabyte.v2.exception.DatabaseException;
import com.yugabyte.v2.exception.ErrorCode;
import com.yugabyte.v2.exception.ValidationException;
import com.yugabyte.v2.service.DatabaseMetadataServiceV2;
import com.yugabyte.v2.service.DatabaseMetadataServiceV2.ColumnMetadata;
import com.yugabyte.v2.util.DataTypeValidator;
import com.yugabyte.v2.util.ResponseUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * CRUD处理器V2
 * 增强错误处理和响应标准化
 * 重构版本：提取公共代码，减少函数臃肿
 */
public class CrudHandlerV2 {
    
    private static final Gson gson = new GsonBuilder()
        .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        .create();
    
    /**
     * 处理HTTP GET请求
     */
    public HttpResponseMessage handleGet(HttpRequestMessage<Optional<String>> request, Connection conn, 
                                         String table, ExecutionContext context) throws SQLException {
        Map<String, String> queryParams = request.getQueryParameters();
        
        // 1. 获取表的元数据
        Map<String, ColumnMetadata> tableColumns = DatabaseMetadataServiceV2.getTableColumns(conn, table, context);
        
        // 2. 构建SQL语句
        StringBuilder sql = new StringBuilder("SELECT * FROM " + table);
        List<Object> params = new ArrayList<>();
        
        // 构建WHERE子句
        String whereClause = buildWhereClause(queryParams, tableColumns, table, params);
        if (!whereClause.isEmpty()) {
            sql.append(" WHERE ").append(whereClause);
        }
        
        context.getLogger().info("执行GET查询: " + sql.toString());
        
        // 3. 执行查询
        try (PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(i + 1, params.get(i));
            }
            
            ResultSet rs = pstmt.executeQuery();
            List<Map<String, Object>> resultList = convertResultSetToList(rs);
            
            return ResponseUtil.success(request, resultList);
        } catch (SQLException e) {
            context.getLogger().severe("数据库查询错误: " + e.getMessage());
            throw DatabaseException.queryFailed("SELECT", e.getMessage());
        }
    }
    
    /**
     * 处理HTTP POST请求
     */
    public HttpResponseMessage handlePost(HttpRequestMessage<Optional<String>> request, Connection conn, 
                                          String table, ExecutionContext context) throws SQLException {
        String jsonBody = request.getBody().orElse(null);
        if (jsonBody == null || jsonBody.isEmpty()) {
            return ResponseUtil.validationError(request, ErrorCode.MISSING_BODY, "request_body", "请求体不能为空");
        }
        
        JsonObject body = gson.fromJson(jsonBody, JsonObject.class);
        
        // 1. 获取表的元数据和主键
        Map<String, ColumnMetadata> tableColumns = DatabaseMetadataServiceV2.getTableColumns(conn, table, context);
        String primaryKeyColumn = DatabaseMetadataServiceV2.getPrimaryKeyColumnName(conn, table, context);
        
        if (primaryKeyColumn == null) {
            throw DatabaseException.noPrimaryKey(table);
        }
        
        // 获取时间戳列名
        String timestampColumn = DatabaseMetadataServiceV2.findAutoTimestampColumn(tableColumns);
        
        // 2. 验证请求体
        validateRequestBodyForInsert(body, tableColumns, primaryKeyColumn, timestampColumn, table);
        
        // 3. 验证数据类型
        validateDataTypes(body, tableColumns, primaryKeyColumn, timestampColumn, table);
        
        // 4. 准备构建SQL
        UUID newId = UUID.randomUUID();
        InsertSqlBuilder insertBuilder = new InsertSqlBuilder(table, primaryKeyColumn, newId);
        
        // 添加请求体中的字段
        for (String key : body.keySet()) {
            if (key.equals(primaryKeyColumn) || key.equals(timestampColumn)) {
                continue;
            }
            if (tableColumns.containsKey(key)) {
                ColumnMetadata meta = tableColumns.get(key);
                JsonElement value = body.get(key);
                insertBuilder.addColumn(key, value, meta.typeName());
            }
        }
        
        // 添加时间戳
        handleAutoTimestampForInsert(tableColumns, insertBuilder);
        
        // 5. 执行插入
        String sql = insertBuilder.build();
        context.getLogger().info("执行POST插入: " + sql);
        
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            insertBuilder.setParameters(pstmt);
            int affectedRows = pstmt.executeUpdate();
            
            if (affectedRows > 0) {
                Map<String, Object> response = new HashMap<>();
                response.put("id", newId.toString());
                response.put("status", "created");
                response.put("table", table);
                return ResponseUtil.success(request, response);
            } else {
                throw new SQLException("插入操作未影响任何行");
            }
        } catch (SQLException e) {
            context.getLogger().severe("数据库插入错误: " + e.getMessage());
            throw DatabaseException.queryFailed("INSERT", e.getMessage());
        }
    }
    
    /**
     * 处理HTTP PATCH请求
     */
    public HttpResponseMessage handlePatch(HttpRequestMessage<Optional<String>> request, Connection conn, 
                                           String table, ExecutionContext context) throws SQLException {
        String jsonBody = request.getBody().orElse(null);
        if (jsonBody == null || jsonBody.isEmpty()) {
            return ResponseUtil.validationError(request, ErrorCode.MISSING_BODY, "request_body", "请求体不能为空");
        }
        
        Map<String, String> queryParams = request.getQueryParameters();
        if (queryParams.isEmpty()) {
            return ResponseUtil.validationError(request, ErrorCode.MISSING_FILTER, "query_parameters", "PATCH请求需要查询参数进行过滤");
        }
        
        // 1. 获取表的元数据
        Map<String, ColumnMetadata> tableColumns = DatabaseMetadataServiceV2.getTableColumns(conn, table, context);
        String primaryKeyColumn = DatabaseMetadataServiceV2.getPrimaryKeyColumnName(conn, table, context);
        
        JsonObject body = gson.fromJson(jsonBody, JsonObject.class);
        String timestampColumn = DatabaseMetadataServiceV2.findAutoTimestampColumn(tableColumns);
        
        // 2. 验证请求体中的字段是否都在表中存在
        validateColumnsExist(body.keySet(), tableColumns, primaryKeyColumn, timestampColumn, table, "请求体");
        validateColumnsExist(queryParams.keySet(), tableColumns, null, null, table, "查询参数");
        
        // 3. 验证非空字段是否被修改为空值
        validateNotNullFieldsForUpdate(body, tableColumns, table);
        
        // 4. 验证数据类型
        validateDataTypes(body, tableColumns, primaryKeyColumn, timestampColumn, table);
        
        // 5. 构建SET子句
        UpdateSqlBuilder updateBuilder = new UpdateSqlBuilder(table);
        
        for (String key : body.keySet()) {
            if (key.equals(primaryKeyColumn) || key.equals(timestampColumn)) {
                continue;
            }
            if (tableColumns.containsKey(key) && !key.equals(primaryKeyColumn)) {
                ColumnMetadata meta = tableColumns.get(key);
                JsonElement value = body.get(key);
                updateBuilder.addSetClause(key, value, meta.typeName());
            }
        }
        
        handleAutoTimestampForUpdate(tableColumns, updateBuilder);
        
        if (updateBuilder.isEmpty()) {
            return ResponseUtil.validationError(request, 
                ErrorCode.NO_VALID_COLUMNS, 
                "request_body", 
                "请求体中未提供有效的列进行更新");
        }
        
        // 6. 构建WHERE子句
        List<Object> whereParams = new ArrayList<>();
        String whereClause = buildWhereClause(queryParams, tableColumns, table, whereParams);
        
        if (whereClause.isEmpty()) {
            return ResponseUtil.validationError(request, 
                ErrorCode.NO_VALID_FILTERS, 
                "query_parameters", 
                "未提供有效的查询参数进行过滤");
        }
        
        // 7. 执行更新
        String sql = updateBuilder.build(whereClause);
        context.getLogger().info("执行PATCH更新: " + sql);
        
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            updateBuilder.setParameters(pstmt, whereParams);
            int affectedRows = pstmt.executeUpdate();
            
            Map<String, Object> response = new HashMap<>();
            response.put("rowsAffected", affectedRows);
            response.put("table", table);
            return ResponseUtil.success(request, response);
        } catch (SQLException e) {
            context.getLogger().severe("数据库更新错误: " + e.getMessage());
            throw DatabaseException.queryFailed("UPDATE", e.getMessage());
        }
    }
    
    /**
     * 处理HTTP DELETE请求
     */
    public HttpResponseMessage handleDelete(HttpRequestMessage<Optional<String>> request, Connection conn, 
                                            String table, ExecutionContext context) throws SQLException {
        Map<String, String> queryParams = request.getQueryParameters();
        if (queryParams.isEmpty()) {
            return ResponseUtil.validationError(request, 
                ErrorCode.MISSING_FILTER, 
                "query_parameters", 
                "DELETE请求需要查询参数进行过滤");
        }
        
        // 1. 获取表的元数据
        Map<String, ColumnMetadata> tableColumns = DatabaseMetadataServiceV2.getTableColumns(conn, table, context);
        
        // 2. 验证查询参数中的字段是否都在表中存在
        validateColumnsExist(queryParams.keySet(), tableColumns, null, null, table, "查询参数");
        
        // 3. 构建WHERE子句并执行删除
        List<Object> params = new ArrayList<>();
        String whereClause = buildWhereClause(queryParams, tableColumns, table, params);
        
        if (whereClause.isEmpty()) {
            return ResponseUtil.validationError(request, 
                ErrorCode.NO_VALID_FILTERS, 
                "query_parameters", 
                "未提供有效的查询参数进行过滤");
        }
        
        String sql = String.format("DELETE FROM %s WHERE %s", table, whereClause);
        context.getLogger().info("执行DELETE删除: " + sql);
        
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(i + 1, params.get(i));
            }
            int affectedRows = pstmt.executeUpdate();
            
            Map<String, Object> response = new HashMap<>();
            response.put("rowsAffected", affectedRows);
            response.put("table", table);
            return ResponseUtil.success(request, response);
        } catch (SQLException e) {
            context.getLogger().severe("数据库删除错误: " + e.getMessage());
            throw DatabaseException.queryFailed("DELETE", e.getMessage());
        }
    }
    
    // ================ 公共辅助方法 ================
    
    /**
     * 构建WHERE子句
     */
    private String buildWhereClause(Map<String, String> queryParams, Map<String, ColumnMetadata> tableColumns,
                                   String table, List<Object> params) {
        List<String> whereClauses = new ArrayList<>();
        
        for (Map.Entry<String, String> entry : queryParams.entrySet()) {
            String key = entry.getKey();
            if (tableColumns.containsKey(key)) {
                whereClauses.add(key + " = ?");
                try {
                    params.add(UUID.fromString(entry.getValue()));
                } catch (IllegalArgumentException e) {
                    params.add(entry.getValue());
                }
            } else {
                throw ValidationException.unknownColumn(key, table);
            }
        }
        
        return String.join(" AND ", whereClauses);
    }
    
    /**
     * 验证字段集合是否都在表中存在
     */
    private void validateColumnsExist(Iterable<String> columns, Map<String, ColumnMetadata> tableColumns,
                                     String primaryKeyColumn, String timestampColumn, 
                                     String table, String source) {
        List<String> unknownColumns = new ArrayList<>();
        
        for (String key : columns) {
            if ((primaryKeyColumn != null && key.equals(primaryKeyColumn)) || 
                (timestampColumn != null && key.equals(timestampColumn))) {
                continue;
            }
            if (!tableColumns.containsKey(key)) {
                unknownColumns.add(key);
            }
        }
        
        if (!unknownColumns.isEmpty()) {
            throw ValidationException.unknownColumn(unknownColumns.get(0), table);
        }
    }
    
    /**
     * 验证INSERT请求体
     */
    private void validateRequestBodyForInsert(JsonObject body, Map<String, ColumnMetadata> tableColumns,
                                            String primaryKeyColumn, String timestampColumn, String table) {
        // 检查所有非空字段是否都已提供
        List<String> missingFields = new ArrayList<>();
        for (Map.Entry<String, ColumnMetadata> entry : tableColumns.entrySet()) {
            String columnName = entry.getKey();
            if (!entry.getValue().isNullable() && 
                !columnName.equals(primaryKeyColumn) && 
                !columnName.equals(timestampColumn) && 
                !body.has(columnName)) {
                missingFields.add(columnName);
            }
        }
        
        if (!missingFields.isEmpty()) {
            throw ValidationException.missingRequiredField(String.join(", ", missingFields));
        }
        
        // 验证非空字段是否提供了空值
        List<String> nullRequiredFields = new ArrayList<>();
        for (Map.Entry<String, ColumnMetadata> entry : tableColumns.entrySet()) {
            String columnName = entry.getKey();
            if (!entry.getValue().isNullable() && 
                !columnName.equals(primaryKeyColumn) && 
                !columnName.equals(timestampColumn) && 
                body.has(columnName)) {
                
                JsonElement value = body.get(columnName);
                if (value == null || value.isJsonNull()) {
                    nullRequiredFields.add(columnName);
                }
            }
        }
        
        if (!nullRequiredFields.isEmpty()) {
            throw ValidationException.notNull(nullRequiredFields.get(0));
        }
    }
    
    /**
     * 验证UPDATE请求体中的非空字段
     */
    private void validateNotNullFieldsForUpdate(JsonObject body, Map<String, ColumnMetadata> tableColumns,
                                               String table) {
        List<String> nullRequiredFields = new ArrayList<>();
        for (String key : body.keySet()) {
            if (tableColumns.containsKey(key)) {
                ColumnMetadata meta = tableColumns.get(key);
                if (!meta.isNullable() && body.has(key)) {
                    JsonElement value = body.get(key);
                    if (value == null || value.isJsonNull()) {
                        nullRequiredFields.add(key);
                    }
                }
            }
        }
        
        if (!nullRequiredFields.isEmpty()) {
            throw ValidationException.notNull(nullRequiredFields.get(0));
        }
    }
    
    /**
     * 验证数据类型
     */
    private void validateDataTypes(JsonObject body, Map<String, ColumnMetadata> tableColumns,
                                  String primaryKeyColumn, String timestampColumn, String table) {
        for (String key : body.keySet()) {
            if ((primaryKeyColumn != null && key.equals(primaryKeyColumn)) || 
                (timestampColumn != null && key.equals(timestampColumn))) {
                continue;
            }
            if (tableColumns.containsKey(key)) {
                ColumnMetadata columnMetadata = tableColumns.get(key);
                JsonElement value = body.get(key);
                
                if (value != null && !value.isJsonNull()) {
                    DataTypeValidator.validateDataType(key, columnMetadata, value, table);
                }
            }
        }
    }
    
    /**
     * 转换ResultSet为List<Map>
     */
    private List<Map<String, Object>> convertResultSetToList(ResultSet rs) throws SQLException {
        List<Map<String, Object>> resultList = new ArrayList<>();
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        
        while (rs.next()) {
            Map<String, Object> row = new HashMap<>();
            for (int i = 1; i <= columns; ++i) {
                String columnName = md.getColumnName(i);
                String columnTypeName = md.getColumnTypeName(i);
                
                if ("jsonb".equalsIgnoreCase(columnTypeName) || "json".equalsIgnoreCase(columnTypeName)) {
                    String jsonString = rs.getString(i);
                    if (jsonString != null) {
                        row.put(columnName, gson.fromJson(jsonString, Object.class));
                    } else {
                        row.put(columnName, null);
                    }
                } else {
                    row.put(columnName, rs.getObject(i));
                }
            }
            resultList.add(row);
        }
        
        return resultList;
    }
    
    /**
     * 为INSERT操作处理自动时间戳
     */
    private void handleAutoTimestampForInsert(Map<String, ColumnMetadata> tableColumns, InsertSqlBuilder builder) {
        String timestampColumn = DatabaseMetadataServiceV2.findAutoTimestampColumn(tableColumns);
        if (timestampColumn != null) {
            builder.addTimestampColumn(timestampColumn);
        }
    }
    
    /**
     * 为UPDATE操作处理自动时间戳
     */
    private void handleAutoTimestampForUpdate(Map<String, ColumnMetadata> tableColumns, UpdateSqlBuilder builder) {
        String timestampColumn = DatabaseMetadataServiceV2.findAutoTimestampColumn(tableColumns);
        if (timestampColumn != null) {
            builder.addTimestampColumn(timestampColumn);
        }
    }
    
    // ================ 内部辅助类 ================
    
    /**
     * INSERT SQL构建器
     */
    private static class InsertSqlBuilder {
        private final String table;
        private final String primaryKeyColumn;
        private final UUID primaryKeyValue;
        private final List<String> columns = new ArrayList<>();
        private final List<String> placeholders = new ArrayList<>();
        private final List<Object> params = new ArrayList<>();
        
        public InsertSqlBuilder(String table, String primaryKeyColumn, UUID primaryKeyValue) {
            this.table = table;
            this.primaryKeyColumn = primaryKeyColumn;
            this.primaryKeyValue = primaryKeyValue;
            
            // 添加主键
            columns.add(primaryKeyColumn);
            placeholders.add("?");
            params.add(primaryKeyValue);
        }
        
        public void addColumn(String columnName, JsonElement value, String typeName) {
            columns.add(columnName);
            params.add(value);
            
            if ("jsonb".equalsIgnoreCase(typeName)) {
                placeholders.add("?::jsonb");
            } else {
                placeholders.add("?");
            }
        }
        
        public void addTimestampColumn(String timestampColumn) {
            columns.add(timestampColumn);
            placeholders.add("NOW()");
        }
        
        public String build() {
            return String.format("INSERT INTO %s (%s) VALUES (%s)",
                    table,
                    String.join(", ", columns),
                    String.join(", ", placeholders));
        }
        
        public void setParameters(PreparedStatement pstmt) throws SQLException {
            for (int i = 0; i < params.size(); i++) {
                Object paramValue = params.get(i);
                if (paramValue instanceof UUID) {
                    pstmt.setObject(i + 1, paramValue);
                } else if (paramValue instanceof JsonElement) {
                    JsonElement value = (JsonElement) paramValue;
                    if (value.isJsonNull()) {
                        pstmt.setNull(i + 1, Types.VARCHAR);
                    } else if (value.isJsonObject() || value.isJsonArray()) {
                        pstmt.setString(i + 1, value.toString());
                    } else {
                        pstmt.setString(i + 1, value.getAsString());
                    }
                }
            }
        }
    }
    
    /**
     * UPDATE SQL构建器
     */
    private static class UpdateSqlBuilder {
        private final String table;
        private final List<String> setClauses = new ArrayList<>();
        private final List<Object> setParams = new ArrayList<>();
        
        public UpdateSqlBuilder(String table) {
            this.table = table;
        }
        
        public void addSetClause(String columnName, JsonElement value, String typeName) {
            if ("jsonb".equalsIgnoreCase(typeName)) {
                setClauses.add(columnName + " = ?::jsonb");
            } else {
                setClauses.add(columnName + " = ?");
            }
            
            if (value.isJsonNull()) {
                setParams.add(null);
            } else if (value.isJsonObject() || value.isJsonArray()) {
                setParams.add(value.toString());
            } else {
                setParams.add(value.getAsString());
            }
        }
        
        public void addTimestampColumn(String timestampColumn) {
            setClauses.add(timestampColumn + " = NOW()");
        }
        
        public boolean isEmpty() {
            return setClauses.isEmpty();
        }
        
        public String build(String whereClause) {
            return String.format("UPDATE %s SET %s WHERE %s",
                    table,
                    String.join(", ", setClauses),
                    whereClause);
        }
        
        public void setParameters(PreparedStatement pstmt, List<Object> whereParams) throws SQLException {
            // 设置SET参数
            for (int i = 0; i < setParams.size(); i++) {
                Object paramValue = setParams.get(i);
                pstmt.setObject(i + 1, paramValue);
            }
            
            // 设置WHERE参数
            for (int i = 0; i < whereParams.size(); i++) {
                pstmt.setObject(setParams.size() + i + 1, whereParams.get(i));
            }
        }
    }
}
