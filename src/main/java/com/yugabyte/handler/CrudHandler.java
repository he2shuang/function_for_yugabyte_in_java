package com.yugabyte.handler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.yugabyte.service.DatabaseMetadataService;
import com.yugabyte.service.DatabaseMetadataService.ColumnMetadata;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * 包含所有 CRUD (创建, 读取, 更新, 删除) 操作核心业务逻辑的处理器类。
 * 该类严重依赖 {@link DatabaseMetadataService} 来动态地适应不同的数据库表结构。
 */
public class CrudHandler {

    final static String TARGET_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    private static final Gson gson = new GsonBuilder()
        .setDateFormat(TARGET_DATE_FORMAT)
        .create();

    /**
     * 处理 HTTP GET 请求，用于查询和检索记录。
     *
     * @param request 包含请求信息的 {@link HttpRequestMessage} 对象。
     * @param conn    一个活动的数据库连接。
     * @param table   操作的目标表名。
     * @param context Azure Function 的执行上下文。
     * @return 一个包含查询结果的 HTTP 响应。
     * @throws SQLException 如果发生数据库访问错误。
     */
    public HttpResponseMessage handleGet(HttpRequestMessage<Optional<String>> request, Connection conn, String table, ExecutionContext context) throws SQLException {
        Map<String, String> queryParams = request.getQueryParameters();
        
        // 1. 获取表的元数据用于验证
        Map<String, ColumnMetadata> tableColumns = DatabaseMetadataService.getTableColumns(conn, table, context);
        
        // 2. 构建 SQL 语句和 WHERE 子句
        StringBuilder sql = new StringBuilder("SELECT * FROM " + table);
        List<Object> params = new ArrayList<>();
        List<String> whereClauses = new ArrayList<>();

        // 处理查询参数，将查询参数转换为 WHERE 子句
        if (!queryParams.isEmpty()) {
            for (Map.Entry<String, String> entry : queryParams.entrySet()) {
                String key = entry.getKey();
                // 只处理在表中实际存在的列，如果存在其他查询参数，给出提醒
                if (tableColumns.containsKey(key)) {
                    whereClauses.add(key + " = ?");
                    try {
                        // 使用 UUID 转换尝试
                        params.add(UUID.fromString(entry.getValue()));
                    } catch (IllegalArgumentException e) {
                        params.add(entry.getValue());
                    }
                } else {
                    context.getLogger().warning("Ignoring invalid query parameter '" + key + "' as it's not a column in table '" + table + "'.");
                }
            }

            if (!whereClauses.isEmpty()) {
                sql.append(" WHERE ").append(String.join(" AND ", whereClauses));
            }
        }

        context.getLogger().info("Executing GET: " + sql.toString());

        // 3. 执行查询
        try (PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(i + 1, params.get(i));
            }

            ResultSet rs = pstmt.executeQuery();
            List<Map<String, Object>> resultList = new ArrayList<>();
            ResultSetMetaData md = rs.getMetaData();
            int columns = md.getColumnCount();
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columns; ++i) {
                    // // 对于 jsonb 类型，getObject 可能会返回 PGobject，其 getValue() 方法返回 json 字符串
                    // Object obj = rs.getObject(i);
                    // if (obj instanceof org.postgresql.util.PGobject && "jsonb".equalsIgnoreCase(((org.postgresql.util.PGobject) obj).getType())) {
                    //     // 使用 Gson 将 json 字符串解析回 Java 对象，使输出更美观
                    //     row.put(md.getColumnName(i), gson.fromJson(((org.postgresql.util.PGobject) obj).getValue(), Object.class));
                    // } else {
                    //     row.put(md.getColumnName(i), obj);
                    // }

                    String columnName = md.getColumnName(i);
                    String columnTypeName = md.getColumnTypeName(i); 

                    if ("jsonb".equalsIgnoreCase(columnTypeName) || "json".equalsIgnoreCase(columnTypeName)) {
                        // 对于 json/jsonb 类型，最安全的方式是将其作为字符串获取
                        String jsonString = rs.getString(i);
                        
                        // 避免 jsonString 为 null 时 gson 抛出异常
                        if (jsonString != null) {
                            // 使用 Gson 将 json 字符串解析回 Java 对象
                            row.put(columnName, gson.fromJson(jsonString, Object.class));
                        } else {
                            row.put(columnName, null);
                        }
                    } else {
                        // 对于所有其他类型，getObject() 仍然是最佳选择
                        row.put(columnName, rs.getObject(i));
                    }
                }
                resultList.add(row);
            }
            return request.createResponseBuilder(HttpStatus.OK).header("Content-Type", "application/json").body(gson.toJson(resultList)).build();
        }
    }

    /**
     * 处理 HTTP POST 请求，用于创建一条新纪录。
     *
     * @param request 包含请求信息的 {@link HttpRequestMessage} 对象。
     * @param conn    一个活动的数据库连接。
     * @param table   操作的目标表名。
     * @param context Azure Function 的执行上下文。
     * @return 一个包含新记录 ID 的 HTTP 响应；如果验证失败，则返回 400 Bad Request。
     * @throws SQLException 如果发生数据库访问错误。
     */
    public HttpResponseMessage handlePost(HttpRequestMessage<Optional<String>> request, Connection conn, String table, ExecutionContext context) throws SQLException {
        String jsonBody = request.getBody().orElse(null);
        if (jsonBody == null || jsonBody.isEmpty()) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Request body is required for POST.").build();
        }

        JsonObject body = gson.fromJson(jsonBody, JsonObject.class);
        
        // 1. 从数据库获取表的元数据和主键;
        // 这里默认主键为UUID，需要在插入数据前生成
        Map<String, ColumnMetadata> tableColumns = DatabaseMetadataService.getTableColumns(conn, table, context);
        String primaryKeyColumn = DatabaseMetadataService.getPrimaryKeyColumnName(conn, table, context);

        if (primaryKeyColumn == null) {
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Table '" + table + "' does not have a primary key defined, cannot proceed.").build();
        }
        
        // 获取时间戳列名 
        String timestampColumn = DatabaseMetadataService.findAutoTimestampColumn(tableColumns);

        // 2. 验证：检查所有非空字段是否都已提供
        List<String> missingFields = new ArrayList<>();
        for (Map.Entry<String, ColumnMetadata> entry : tableColumns.entrySet()) {
            String columnName = entry.getKey();

            // 如果一个列是 NOT NULL，它不是主键或者时间戳（我们会自己生成），并且请求体里也没有提供它，需要提醒用户少了什么
            if (!entry.getValue().isNullable() && !columnName.equals(primaryKeyColumn) && !columnName.equals(timestampColumn) && !body.has(columnName)) {
                missingFields.add(columnName);
            }
        }

        if (!missingFields.isEmpty()) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .body("Missing required fields: " + String.join(", ", missingFields)).build();
        }
        
        // 3. 准备构建 SQL
        List<String> columnsToInsert = new ArrayList<>();
        List<String> valuePlaceholders = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        
        // 首先，添加由函数生成的 UUID 主键
        UUID newId = UUID.randomUUID();
        columnsToInsert.add(primaryKeyColumn);
        valuePlaceholders.add("?");
        params.add(newId);
        
        // 遍历请求体中的字段，只处理那些在数据库表中实际存在的列
        for (String key : body.keySet()) {
            if (key.equals(primaryKeyColumn) || key.equals(timestampColumn)) {
                continue;
            }
            if (tableColumns.containsKey(key)) {
                columnsToInsert.add(key);
                params.add(body.get(key)); 
                
                // 检查是否为jsonb类型
                ColumnMetadata meta = tableColumns.get(key);
                if ("jsonb".equalsIgnoreCase(meta.typeName())) {
                    valuePlaceholders.add("?::jsonb");
                } else {
                    valuePlaceholders.add("?");
                }
            }
        }

        // 添加由函数生成的时间戳
        handleAutoTimestampForInsert(tableColumns, columnsToInsert, valuePlaceholders);

        // 4. 构建 SQL 语句
        String sql = String.format("INSERT INTO %s (%s) VALUES (%s)",
                table,
                String.join(", ", columnsToInsert),
                String.join(", ", valuePlaceholders));
        
        context.getLogger().info("Executing POST: " + sql);
        
        // 5. 执行插入
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
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
            
            int affectedRows = pstmt.executeUpdate();
            
            if (affectedRows > 0) {
                 return request.createResponseBuilder(HttpStatus.CREATED)
                        .body("status\": \"created\"}")
                        .build();
            } else {
                return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body("Failed to create record.").build();
            }
        }
    }

    /**
     * 处理 HTTP PATCH 请求，用于部分更新现有记录。
     *
     * @param request 包含请求信息的 {@link HttpRequestMessage} 对象。
     * @param conn    一个活动的数据库连接。
     * @param table   操作的目标表名。
     * @param context Azure Function 的执行上下文。
     * @return 一个包含受影响行数的 HTTP 响应；如果验证失败，则返回 400 Bad Request。
     * @throws SQLException 如果发生数据库访问错误。
     */
    public HttpResponseMessage handlePatch(HttpRequestMessage<Optional<String>> request, Connection conn, String table, ExecutionContext context) throws SQLException {
        String jsonBody = request.getBody().orElse(null);
        if (jsonBody == null || jsonBody.isEmpty()) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Request body is required for PATCH.").build();
        }
        
        Map<String, String> queryParams = request.getQueryParameters();
        if (queryParams.isEmpty()) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("PATCH requests require query parameters for filtering (WHERE clause).").build();
        }

        // 1. 获取表的元数据
        Map<String, ColumnMetadata> tableColumns = DatabaseMetadataService.getTableColumns(conn, table, context);
        String primaryKeyColumn = DatabaseMetadataService.getPrimaryKeyColumnName(conn, table, context);

        JsonObject body = gson.fromJson(jsonBody, JsonObject.class);
        
        String timestampColumn = DatabaseMetadataService.findAutoTimestampColumn(tableColumns);

        // 2. 构建 SET 子句
        List<String> setClauses = new ArrayList<>();
        List<Object> setParams = new ArrayList<>();
        for (String key : body.keySet()) {
            if (key.equals(primaryKeyColumn) || key.equals(timestampColumn)) {
                continue;
            }
            // 只处理在表中存在且不是主键的列
            if (tableColumns.containsKey(key) && !key.equals(primaryKeyColumn)) {
                JsonElement value = body.get(key);
                ColumnMetadata meta = tableColumns.get(key);

                if ("jsonb".equalsIgnoreCase(meta.typeName())) {
                    setClauses.add(key + " = ?::jsonb");
                } else {
                    setClauses.add(key + " = ?");
                }
                
                if (value.isJsonNull()) {
                    setParams.add(null);
                } else if (value.isJsonObject() || value.isJsonArray()) {
                    setParams.add(value.toString());
                } else {
                    setParams.add(value.getAsString());
                }
            }
        }

        handleAutoTimestampForUpdate(tableColumns, setClauses);
        
        if (setClauses.isEmpty()) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("No valid columns to update were provided in the request body.").build();
        }
        
        // 3. 构建 WHERE 子句
        List<String> whereClauses = new ArrayList<>();
        List<Object> whereParams = new ArrayList<>();
        for (Map.Entry<String, String> entry : queryParams.entrySet()) {
            // 只处理在表中存在的列
            if (tableColumns.containsKey(entry.getKey())) {
                whereClauses.add(entry.getKey() + " = ?");
                try {
                    // 使用 UUID 转换尝试
                    whereParams.add(UUID.fromString(entry.getValue()));
                } catch (IllegalArgumentException e) {
                    whereParams.add(entry.getValue());
                }
            }
        }

        if (whereClauses.isEmpty()) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("No valid query parameters provided for filtering.").build();
        }

        // 4. 合并参数并构建最终 SQL
        List<Object> allParams = new ArrayList<>(setParams);
        allParams.addAll(whereParams);

        String sql = String.format("UPDATE %s SET %s WHERE %s",
                table,
                String.join(", ", setClauses),
                String.join(" AND ", whereClauses));

        context.getLogger().info("Executing PATCH: " + sql);
        
        // 5. 执行更新
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < allParams.size(); i++) {
                pstmt.setObject(i + 1, allParams.get(i));
            }
            int affectedRows = pstmt.executeUpdate();
            return request.createResponseBuilder(HttpStatus.OK).body("{\"rowsAffected\": " + affectedRows + "}").build();
        }
    }
    
    /**
     * 处理 HTTP DELETE 请求，用于删除记录。
     *
     * @param request 包含请求信息的 {@link HttpRequestMessage} 对象。
     * @param conn    一个活动的数据库连接。
     * @param table   操作的目标表名。
     * @param context Azure Function 的执行上下文。
     * @return 一个包含受影响行数的 HTTP 响应；如果未提供过滤条件，则返回 400 Bad Request。
     * @throws SQLException 如果发生数据库访问错误。
     */
    public HttpResponseMessage handleDelete(HttpRequestMessage<Optional<String>> request, Connection conn, String table, ExecutionContext context) throws SQLException {
        Map<String, String> queryParams = request.getQueryParameters();
        if (queryParams.isEmpty()) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("DELETE requests require query parameters for filtering. Unfiltered deletes are not allowed.").build();
        }

        // 1. 获取表的元数据用于验证
        Map<String, ColumnMetadata> tableColumns = DatabaseMetadataService.getTableColumns(conn, table, context);
        
        // 2. 构建 WHERE 子句
        List<String> whereClauses = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        for (Map.Entry<String, String> entry : queryParams.entrySet()) {
            // 只处理在表中存在的列
            if (tableColumns.containsKey(entry.getKey())) {
                whereClauses.add(entry.getKey() + " = ?");
                try {
                    params.add(UUID.fromString(entry.getValue()));
                } catch (IllegalArgumentException e) {
                    params.add(entry.getValue());
                }
            }
        }
        
        if (whereClauses.isEmpty()) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("No valid query parameters provided for filtering.").build();
        }

        // 3. 构建 SQL
        String sql = String.format("DELETE FROM %s WHERE %s",
                table,
                String.join(" AND ", whereClauses));

        context.getLogger().info("Executing DELETE: " + sql);
        
        // 4. 执行删除
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(i + 1, params.get(i));
            }
            int affectedRows = pstmt.executeUpdate();
            return request.createResponseBuilder(HttpStatus.OK).body("{\"rowsAffected\": " + affectedRows + "}").build();
        }
    }

    
    /**
     * 为 INSERT 操作处理自动时间戳。
     * 它会将时间戳列和 NOW() 函数添加到相应的列表中。
     * 
     * @param tableColumns 当前表的元数据。
     * @param columnsToInsert INSERT 语句的列名列表。
     * @param valuePlaceholders INSERT 语句的值占位符列表。
     */
    private void handleAutoTimestampForInsert(Map<String, ColumnMetadata> tableColumns, List<String> columnsToInsert, List<String> valuePlaceholders) {
        String timestampColumn = DatabaseMetadataService.findAutoTimestampColumn(tableColumns);
        if (timestampColumn != null) {
            columnsToInsert.add(timestampColumn);
            valuePlaceholders.add("NOW()");
        }
    }
    
    
    /**
     * 为 UPDATE 操作处理自动时间戳。
     * 它会将 "column = NOW()" 添加到 SET 子句列表中。
     * 
     * @param tableColumns 当前表的元数据。
     * @param setClauses UPDATE 语句的 SET 子句列表。
     */
    private void handleAutoTimestampForUpdate(Map<String, ColumnMetadata> tableColumns, List<String> setClauses) {
        String timestampColumn = DatabaseMetadataService.findAutoTimestampColumn(tableColumns);
        if (timestampColumn != null) {
            setClauses.add(timestampColumn + " = NOW()");
        }
    }
}