package com.yugabyte.crud.infrastructure.persistence;

import com.yugabyte.crud.domain.exception.ValidationException;
import com.yugabyte.crud.domain.repository.DatabaseMetadataRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * クエリビルダー
 * SQLクエリの構築を担当
 */
public class QueryBuilder {
    
    /**
     * INSERTクエリを構築
     */
    public InsertQuery buildInsertQuery(String table, Map<String, Object> data) {
        List<String> columns = new ArrayList<>();
        List<String> placeholders = new ArrayList<>();
        List<Object> parameters = new ArrayList<>();
        
        // 主キーを追加（提供されていない場合は生成）
        String idColumn = "id"; // 実際のアプリケーションではメタデータから取得
        if (!data.containsKey(idColumn)) {
            data.put(idColumn, UUID.randomUUID());
        }
        
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String columnName = entry.getKey();
            Object value = entry.getValue();
            
            columns.add(columnName);
            placeholders.add("?");
            parameters.add(value);
        }
        
        String sql = String.format("INSERT INTO %s (%s) VALUES (%s)",
                table,
                String.join(", ", columns),
                String.join(", ", placeholders));
        
        return new InsertQuery(sql, parameters);
    }
    
    /**
     * SELECTクエリを構築
     */
    public SelectQuery buildSelectQuery(String table, Map<String, String> queryParams) {
        StringBuilder sql = new StringBuilder("SELECT * FROM " + table);
        List<Object> parameters = new ArrayList<>();
        
        if (queryParams != null && !queryParams.isEmpty()) {
            List<String> whereClauses = new ArrayList<>();
            
            for (Map.Entry<String, String> entry : queryParams.entrySet()) {
                String columnName = entry.getKey();
                String value = entry.getValue();
                
                whereClauses.add(columnName + " = ?");
                
                // UUIDの場合はUUIDオブジェクトに変換
                try {
                    parameters.add(UUID.fromString(value));
                } catch (IllegalArgumentException e) {
                    parameters.add(value);
                }
            }
            
            if (!whereClauses.isEmpty()) {
                sql.append(" WHERE ").append(String.join(" AND ", whereClauses));
            }
        }
        
        return new SelectQuery(sql.toString(), parameters);
    }
    
    /**
     * UPDATEクエリを構築
     */
    public UpdateQuery buildUpdateQuery(String table, Map<String, Object> data, 
                                       Map<String, String> queryParams) {
        if (data == null || data.isEmpty()) {
            throw ValidationException.noValidColumns();
        }
        
        if (queryParams == null || queryParams.isEmpty()) {
            throw ValidationException.missingFilter();
        }
        
        StringBuilder sql = new StringBuilder("UPDATE " + table + " SET ");
        List<Object> parameters = new ArrayList<>();
        List<String> setClauses = new ArrayList<>();
        
        // SET句を構築
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String columnName = entry.getKey();
            Object value = entry.getValue();
            
            setClauses.add(columnName + " = ?");
            parameters.add(value);
        }
        
        sql.append(String.join(", ", setClauses));
        
        // WHERE句を構築
        List<String> whereClauses = new ArrayList<>();
        for (Map.Entry<String, String> entry : queryParams.entrySet()) {
            String columnName = entry.getKey();
            String value = entry.getValue();
            
            whereClauses.add(columnName + " = ?");
            
            // UUIDの場合はUUIDオブジェクトに変換
            try {
                parameters.add(UUID.fromString(value));
            } catch (IllegalArgumentException e) {
                parameters.add(value);
            }
        }
        
        sql.append(" WHERE ").append(String.join(" AND ", whereClauses));
        
        return new UpdateQuery(sql.toString(), parameters);
    }
    
    /**
     * DELETEクエリを構築
     */
    public DeleteQuery buildDeleteQuery(String table, Map<String, String> queryParams) {
        if (queryParams == null || queryParams.isEmpty()) {
            throw ValidationException.missingFilter();
        }
        
        StringBuilder sql = new StringBuilder("DELETE FROM " + table + " WHERE ");
        List<Object> parameters = new ArrayList<>();
        List<String> whereClauses = new ArrayList<>();
        
        for (Map.Entry<String, String> entry : queryParams.entrySet()) {
            String columnName = entry.getKey();
            String value = entry.getValue();
            
            whereClauses.add(columnName + " = ?");
            
            // UUIDの場合はUUIDオブジェクトに変換
            try {
                parameters.add(UUID.fromString(value));
            } catch (IllegalArgumentException e) {
                parameters.add(value);
            }
        }
        
        sql.append(String.join(" AND ", whereClauses));
        
        return new DeleteQuery(sql.toString(), parameters);
    }
    
    // クエリ結果クラス
    
    public static class InsertQuery {
        private final String sql;
        private final List<Object> parameters;
        
        public InsertQuery(String sql, List<Object> parameters) {
            this.sql = sql;
            this.parameters = parameters;
        }
        
        public String getSql() {
            return sql;
        }
        
        public List<Object> getParameters() {
            return parameters;
        }
    }
    
    public static class SelectQuery {
        private final String sql;
        private final List<Object> parameters;
        
        public SelectQuery(String sql, List<Object> parameters) {
            this.sql = sql;
            this.parameters = parameters;
        }
        
        public String getSql() {
            return sql;
        }
        
        public List<Object> getParameters() {
            return parameters;
        }
    }
    
    public static class UpdateQuery {
        private final String sql;
        private final List<Object> parameters;
        
        public UpdateQuery(String sql, List<Object> parameters) {
            this.sql = sql;
            this.parameters = parameters;
        }
        
        public String getSql() {
            return sql;
        }
        
        public List<Object> getParameters() {
            return parameters;
        }
    }
    
    public static class DeleteQuery {
        private final String sql;
        private final List<Object> parameters;
        
        public DeleteQuery(String sql, List<Object> parameters) {
            this.sql = sql;
            this.parameters = parameters;
        }
        
        public String getSql() {
            return sql;
        }
        
        public List<Object> getParameters() {
            return parameters;
        }
    }
}
