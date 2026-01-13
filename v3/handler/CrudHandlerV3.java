package com.yugabyte.v3.handler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.yugabyte.v3.exception.DatabaseException;
import com.yugabyte.v3.exception.ErrorCode;
import com.yugabyte.v3.exception.ValidationException;
import com.yugabyte.v3.service.DatabaseMetadataServiceV3;
import com.yugabyte.v3.service.DatabaseMetadataServiceV3.ColumnMetadata;
import com.yugabyte.v3.util.DataTypeValidator;

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
 * CRUDハンドラーV3
 * エラー処理とレスポンス標準化を強化
 * リファクタリング版：共通コードを抽出し、関数の肥大化を削減
 */
public class CrudHandlerV3 {
    
    private static final Gson gson = new GsonBuilder()
        .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        .create();
    
    /**
     * HTTP GETリクエストを処理
     * @return クエリ結果のリスト
     */
    public List<Map<String, Object>> handleGet(HttpRequestMessage<Optional<String>> request, Connection conn, 
                                         String table, ExecutionContext context) throws ValidationException, DatabaseException {
        Map<String, String> queryParams = request.getQueryParameters();
        
        // 1. テーブルのメタデータを取得
        Map<String, ColumnMetadata> tableColumns = DatabaseMetadataServiceV3.getTableColumns(conn, table, context);
        
        // 2. SQL文を構築
        StringBuilder sql = new StringBuilder("SELECT * FROM " + table);
        List<Object> params = new ArrayList<>();
        
        // WHERE句を構築
        String whereClause = buildWhereClause(queryParams, tableColumns, table, params);
        if (!whereClause.isEmpty()) {
            sql.append(" WHERE ").append(whereClause);
        }
        
        context.getLogger().info("GETクエリを実行: " + sql.toString());
        
        // 3. クエリを実行
        try (PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(i + 1, params.get(i));
            }
            
            ResultSet rs = pstmt.executeQuery();
            return convertResultSetToList(rs);
        } catch (SQLException e) {
            context.getLogger().severe("データベースクエリエラー: " + e.getMessage());
            throw DatabaseException.queryFailed("SELECT", e.getMessage(), e);
        }
    }
    
    /**
     * HTTP POSTリクエストを処理
     * @return 作成結果（id, status, table）
     */
    public Map<String, Object> handlePost(HttpRequestMessage<Optional<String>> request, Connection conn, 
                                          String table, ExecutionContext context) throws ValidationException, DatabaseException {
        String jsonBody = request.getBody().orElse(null);
        if (jsonBody == null || jsonBody.isEmpty()) {
            throw new ValidationException(ErrorCode.MISSING_BODY, "request_body", "リクエストボディは空にできません");
        }
        
        JsonObject body = gson.fromJson(jsonBody, JsonObject.class);
        
        // 1. テーブルのメタデータと主キーを取得
        Map<String, ColumnMetadata> tableColumns = DatabaseMetadataServiceV3.getTableColumns(conn, table, context);
        String primaryKeyColumn = DatabaseMetadataServiceV3.getPrimaryKeyColumnName(conn, table, context);
        
        if (primaryKeyColumn == null) {
            throw DatabaseException.noPrimaryKey(table);
        }
        
        // タイムスタンプ列名を取得
        String timestampColumn = DatabaseMetadataServiceV3.findAutoTimestampColumn(tableColumns);
        
        // 2. リクエストボディを検証
        validateRequestBodyForInsert(body, tableColumns, primaryKeyColumn, timestampColumn, table);
        
        // 3. データ型を検証
        validateDataTypes(body, tableColumns, primaryKeyColumn, timestampColumn, table);
        
        // 4. SQL構築の準備
        UUID newId = UUID.randomUUID();
        InsertSqlBuilder insertBuilder = new InsertSqlBuilder(table, primaryKeyColumn, newId);
        
        // リクエストボディのフィールドを追加
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
        
        // タイムスタンプを追加
        handleAutoTimestampForInsert(tableColumns, insertBuilder);
        
        // 5. 挿入を実行
        String sql = insertBuilder.build();
        context.getLogger().info("POST挿入を実行: " + sql);
        
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            insertBuilder.setParameters(pstmt);
            int affectedRows = pstmt.executeUpdate();
            
            if (affectedRows > 0) {
                Map<String, Object> response = new HashMap<>();
                response.put("id", newId.toString());
                response.put("status", "created");
                response.put("table", table);
                return response;
            } else {
                throw DatabaseException.queryFailed("INSERT", "挿入操作が行に影響を与えませんでした");
            }
        } catch (SQLException e) {
            context.getLogger().severe("データベース挿入エラー: " + e.getMessage());
            throw DatabaseException.queryFailed("INSERT", e.getMessage(), e);
        }
    }
    
    /**
     * HTTP PATCHリクエストを処理
     * @return 更新結果（rowsAffected, table）
     */
    public Map<String, Object> handlePatch(HttpRequestMessage<Optional<String>> request, Connection conn, 
                                           String table, ExecutionContext context) throws ValidationException, DatabaseException {
        String jsonBody = request.getBody().orElse(null);
        if (jsonBody == null || jsonBody.isEmpty()) {
            throw new ValidationException(ErrorCode.MISSING_BODY, "request_body", "リクエストボディは空にできません");
        }
        
        Map<String, String> queryParams = request.getQueryParameters();
        if (queryParams.isEmpty()) {
            throw new ValidationException(ErrorCode.MISSING_FILTER, "query_parameters", "PATCHリクエストにはフィルタリングのためのクエリパラメータが必要です");
        }
        
        // 1. テーブルのメタデータを取得
        Map<String, ColumnMetadata> tableColumns = DatabaseMetadataServiceV3.getTableColumns(conn, table, context);
        String primaryKeyColumn = DatabaseMetadataServiceV3.getPrimaryKeyColumnName(conn, table, context);
        
        JsonObject body;
        try {
            body = gson.fromJson(jsonBody, JsonObject.class);
        } catch (com.google.gson.JsonSyntaxException e) {
            throw new ValidationException(
                ErrorCode.INVALID_FORMAT,
                "request_body",
                String.format("无效的JSON格式: %s", e.getMessage())
            );
        }
        String timestampColumn = DatabaseMetadataServiceV3.findAutoTimestampColumn(tableColumns);
        
        // 2. リクエストボディのフィールドがすべてテーブルに存在するか検証
        validateColumnsExist(body.keySet(), tableColumns, primaryKeyColumn, timestampColumn, table, "リクエストボディ");
        validateColumnsExist(queryParams.keySet(), tableColumns, null, null, table, "クエリパラメータ");
        
        // 3. 非nullフィールドがnull値に変更されていないか検証
        validateNotNullFieldsForUpdate(body, tableColumns, table);
        
        // 4. データ型を検証
        validateDataTypes(body, tableColumns, primaryKeyColumn, timestampColumn, table);
        
        // 5. SET句を構築
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
            throw new ValidationException(ErrorCode.NO_VALID_COLUMNS, "request_body", "リクエストボディに更新する有効な列が提供されていません");
        }
        
        // 6. WHERE句を構築
        List<Object> whereParams = new ArrayList<>();
        String whereClause = buildWhereClause(queryParams, tableColumns, table, whereParams);
        
        if (whereClause.isEmpty()) {
            throw new ValidationException(ErrorCode.NO_VALID_FILTERS, "query_parameters", "フィルタリングのための有効なクエリパラメータが提供されていません");
        }
        
        // 7. 更新を実行
        String sql = updateBuilder.build(whereClause);
        context.getLogger().info("PATCH更新を実行: " + sql);
        
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            updateBuilder.setParameters(pstmt, whereParams);
            int affectedRows = pstmt.executeUpdate();
            
            Map<String, Object> response = new HashMap<>();
            response.put("rowsAffected", affectedRows);
            response.put("table", table);
            return response;
        } catch (SQLException e) {
            context.getLogger().severe("データベース更新エラー: " + e.getMessage());
            throw DatabaseException.queryFailed("UPDATE", e.getMessage(), e);
        }
    }
    
    /**
     * HTTP DELETEリクエストを処理
     * @return 削除結果（rowsAffected, table）
     */
    public Map<String, Object> handleDelete(HttpRequestMessage<Optional<String>> request, Connection conn, 
                                            String table, ExecutionContext context) throws ValidationException, DatabaseException {
        Map<String, String> queryParams = request.getQueryParameters();
        if (queryParams.isEmpty()) {
            throw new ValidationException(ErrorCode.MISSING_FILTER, "query_parameters", "DELETEリクエストにはフィルタリングのためのクエリパラメータが必要です");
        }
        
        // 1. テーブルのメタデータを取得
        Map<String, ColumnMetadata> tableColumns = DatabaseMetadataServiceV3.getTableColumns(conn, table, context);
        
        // 2. クエリパラメータのフィールドがすべてテーブルに存在するか検証
        validateColumnsExist(queryParams.keySet(), tableColumns, null, null, table, "クエリパラメータ");
        
        // 3. WHERE句を構築して削除を実行
        List<Object> params = new ArrayList<>();
        String whereClause = buildWhereClause(queryParams, tableColumns, table, params);
        
        if (whereClause.isEmpty()) {
            throw new ValidationException(ErrorCode.NO_VALID_FILTERS, "query_parameters", "フィルタリングのための有効なクエリパラメータが提供されていません");
        }
        
        String sql = String.format("DELETE FROM %s WHERE %s", table, whereClause);
        context.getLogger().info("DELETE削除を実行: " + sql);
        
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(i + 1, params.get(i));
            }
            int affectedRows = pstmt.executeUpdate();
            
            Map<String, Object> response = new HashMap<>();
            response.put("rowsAffected", affectedRows);
            response.put("table", table);
            return response;
        } catch (SQLException e) {
            context.getLogger().severe("データベース削除エラー: " + e.getMessage());
            throw DatabaseException.queryFailed("DELETE", e.getMessage(), e);
        }
    }
    
    // ================ 共通ヘルパーメソッド ================
    
    /**
     * WHERE句を構築
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
     * フィールドセットがすべてテーブルに存在するか検証
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
     * INSERTリクエストボディを検証
     */
    private void validateRequestBodyForInsert(JsonObject body, Map<String, ColumnMetadata> tableColumns,
                                            String primaryKeyColumn, String timestampColumn, String table) {
        // すべての非nullフィールドが提供されているかチェック
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
        
        // 非nullフィールドにnull値が提供されていないか検証
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
     * UPDATEリクエストボディの非nullフィールドを検証
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
     * データ型を検証
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
     * ResultSetをList<Map>に変換
     */
    private List<Map<String, Object>> convertResultSetToList(ResultSet rs) {
        List<Map<String, Object>> resultList = new ArrayList<>();
        try {
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
        } catch (SQLException e) {
            throw new RuntimeException("ResultSet转换失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * INSERT操作の自動タイムスタンプを処理
     */
    private void handleAutoTimestampForInsert(Map<String, ColumnMetadata> tableColumns, InsertSqlBuilder builder) {
        String timestampColumn = DatabaseMetadataServiceV3.findAutoTimestampColumn(tableColumns);
        if (timestampColumn != null) {
            builder.addTimestampColumn(timestampColumn);
        }
    }
    
    /**
     * UPDATE操作の自動タイムスタンプを処理
     */
    private void handleAutoTimestampForUpdate(Map<String, ColumnMetadata> tableColumns, UpdateSqlBuilder builder) {
        String timestampColumn = DatabaseMetadataServiceV3.findAutoTimestampColumn(tableColumns);
        if (timestampColumn != null) {
            builder.addTimestampColumn(timestampColumn);
        }
    }
    
    // ================ 内部ヘルパークラス ================
    
    /**
     * INSERT SQLビルダー
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
            
            // 主キーを追加
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
     * UPDATE SQLビルダー
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
            // SETパラメータを設定
            for (int i = 0; i < setParams.size(); i++) {
                Object paramValue = setParams.get(i);
                pstmt.setObject(i + 1, paramValue);
            }
            
            // WHEREパラメータを設定
            for (int i = 0; i < whereParams.size(); i++) {
                pstmt.setObject(setParams.size() + i + 1, whereParams.get(i));
            }
        }
    }
}

