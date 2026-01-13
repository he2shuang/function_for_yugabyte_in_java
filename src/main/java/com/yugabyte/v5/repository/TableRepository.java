package com.yugabyte.v5.repository;

import com.yugabyte.v5.exception.DatabaseException;
import com.yugabyte.v5.util.QueryBuilder;

import java.sql.*;
import java.util.*;

/**
 * テーブルリポジトリ
 * データベース操作を担当
 */
public class TableRepository {
    
    /**
     * レコードを挿入
     */
    public Map<String, Object> insert(Connection connection, String table, 
                                     Map<String, Object> data, Map<String, String> columnTypes) {
        String sql = QueryBuilder.buildInsertSql(table, data.keySet());
        
        try (PreparedStatement stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            // パラメータを設定
            int paramIndex = 1;
            for (String column : data.keySet()) {
                Object value = data.get(column);
                setParameter(stmt, paramIndex++, value, columnTypes.get(column));
            }
            
            // SQLを実行
            int affectedRows = stmt.executeUpdate();
            
            if (affectedRows == 0) {
                throw DatabaseException.queryFailed("INSERT", "レコードの挿入に失敗しました");
            }
            
            // 生成されたキーを取得
            try (ResultSet generatedKeys = stmt.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    Map<String, Object> result = new HashMap<>(data);
                    result.put("id", generatedKeys.getObject(1));
                    return result;
                } else {
                    return data;
                }
            }
            
        } catch (SQLException e) {
            throw DatabaseException.queryFailed("INSERT", e.getMessage(), e);
        }
    }
    
    /**
     * レコードを選択
     */
    public List<Map<String, Object>> select(Connection connection, String table,
                                           Map<String, String> queryParams, Map<String, String> columnTypes) {
        String sql = QueryBuilder.buildSelectSql(table, queryParams.keySet());
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            // パラメータを設定
            int paramIndex = 1;
            for (String column : queryParams.keySet()) {
                String value = queryParams.get(column);
                setParameter(stmt, paramIndex++, value, columnTypes.get(column));
            }
            
            // SQLを実行
            try (ResultSet rs = stmt.executeQuery()) {
                return convertResultSetToList(rs);
            }
            
        } catch (SQLException e) {
            throw DatabaseException.queryFailed("SELECT", e.getMessage(), e);
        }
    }
    
    /**
     * レコードを更新
     */
    public Map<String, Object> update(Connection connection, String table,
                                     Map<String, Object> data, Map<String, String> queryParams,
                                     Map<String, String> columnTypes) {
        String sql = QueryBuilder.buildUpdateSql(table, data.keySet(), queryParams.keySet());
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            // 更新する列のパラメータを設定
            int paramIndex = 1;
            for (String column : data.keySet()) {
                Object value = data.get(column);
                setParameter(stmt, paramIndex++, value, columnTypes.get(column));
            }
            
            // WHERE条件のパラメータを設定
            for (String column : queryParams.keySet()) {
                String value = queryParams.get(column);
                setParameter(stmt, paramIndex++, value, columnTypes.get(column));
            }
            
            // SQLを実行
            int affectedRows = stmt.executeUpdate();
            
            if (affectedRows == 0) {
                throw DatabaseException.queryFailed("UPDATE", "更新対象のレコードが見つかりません");
            }
            
            return data;
            
        } catch (SQLException e) {
            throw DatabaseException.queryFailed("UPDATE", e.getMessage(), e);
        }
    }
    
    /**
     * レコードを削除
     */
    public Map<String, Object> delete(Connection connection, String table,
                                     Map<String, String> queryParams, Map<String, String> columnTypes) {
        String sql = QueryBuilder.buildDeleteSql(table, queryParams.keySet());
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            // パラメータを設定
            int paramIndex = 1;
            for (String column : queryParams.keySet()) {
                String value = queryParams.get(column);
                setParameter(stmt, paramIndex++, value, columnTypes.get(column));
            }
            
            // SQLを実行
            int affectedRows = stmt.executeUpdate();
            
            if (affectedRows == 0) {
                throw DatabaseException.queryFailed("DELETE", "削除対象のレコードが見つかりません");
            }
            
            Map<String, Object> result = new HashMap<>();
            result.put("affectedRows", affectedRows);
            return result;
            
        } catch (SQLException e) {
            throw DatabaseException.queryFailed("DELETE", e.getMessage(), e);
        }
    }
    
    /**
     * PreparedStatementにパラメータを設定
     */
    private void setParameter(PreparedStatement stmt, int index, Object value, String columnType) 
            throws SQLException {
        if (value == null) {
            stmt.setNull(index, Types.NULL);
        } else if (value instanceof String) {
            stmt.setString(index, (String) value);
        } else if (value instanceof Integer) {
            stmt.setInt(index, (Integer) value);
        } else if (value instanceof Long) {
            stmt.setLong(index, (Long) value);
        } else if (value instanceof Double) {
            stmt.setDouble(index, (Double) value);
        } else if (value instanceof Boolean) {
            stmt.setBoolean(index, (Boolean) value);
        } else if (value instanceof java.sql.Date) {
            stmt.setDate(index, (java.sql.Date) value);
        } else if (value instanceof java.sql.Timestamp) {
            stmt.setTimestamp(index, (java.sql.Timestamp) value);
        } else {
            stmt.setObject(index, value);
        }
    }
    
    /**
     * ResultSetをList<Map<String, Object>>に変換
     */
    private List<Map<String, Object>> convertResultSetToList(ResultSet rs) throws SQLException {
        List<Map<String, Object>> result = new ArrayList<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        
        while (rs.next()) {
            Map<String, Object> row = new HashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                Object value = rs.getObject(i);
                row.put(columnName, value);
            }
            result.add(row);
        }
        
        return result;
    }
}
