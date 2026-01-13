package com.yugabyte.crud.domain.repository;

import com.microsoft.azure.functions.ExecutionContext;
import com.yugabyte.crud.domain.exception.DatabaseException;
import com.yugabyte.crud.infrastructure.persistence.QueryBuilder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * テーブルリポジトリ
 * テーブル操作の永続化を担当
 */
public class TableRepository {
    
    private final QueryBuilder queryBuilder;
    
    public TableRepository() {
        this.queryBuilder = new QueryBuilder();
    }
    
    /**
     * レコードを挿入
     */
    public Map<String, Object> insert(Connection connection, String table,
                                     Map<String, Object> data, ExecutionContext context) {
        
        QueryBuilder.InsertQuery insertQuery = queryBuilder.buildInsertQuery(table, data);
        String sql = insertQuery.getSql();
        List<Object> parameters = insertQuery.getParameters();
        
        context.getLogger().info("INSERTクエリを実行: " + sql);
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            setParameters(pstmt, parameters);
            int affectedRows = pstmt.executeUpdate();
            
            if (affectedRows > 0) {
                Map<String, Object> response = new HashMap<>();
                response.put("id", data.get("id") != null ? data.get("id").toString() : UUID.randomUUID().toString());
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
     * レコードを検索
     */
    public List<Map<String, Object>> select(Connection connection, String table,
                                           Map<String, String> queryParams, ExecutionContext context) {
        
        QueryBuilder.SelectQuery selectQuery = queryBuilder.buildSelectQuery(table, queryParams);
        String sql = selectQuery.getSql();
        List<Object> parameters = selectQuery.getParameters();
        
        context.getLogger().info("SELECTクエリを実行: " + sql);
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            setParameters(pstmt, parameters);
            ResultSet rs = pstmt.executeQuery();
            return convertResultSetToList(rs);
        } catch (SQLException e) {
            context.getLogger().severe("データベース検索エラー: " + e.getMessage());
            throw DatabaseException.queryFailed("SELECT", e.getMessage(), e);
        }
    }
    
    /**
     * レコードを更新
     */
    public Map<String, Object> update(Connection connection, String table,
                                     Map<String, Object> data, Map<String, String> queryParams,
                                     ExecutionContext context) {
        
        QueryBuilder.UpdateQuery updateQuery = queryBuilder.buildUpdateQuery(table, data, queryParams);
        String sql = updateQuery.getSql();
        List<Object> parameters = updateQuery.getParameters();
        
        context.getLogger().info("UPDATEクエリを実行: " + sql);
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            setParameters(pstmt, parameters);
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
     * レコードを削除
     */
    public Map<String, Object> delete(Connection connection, String table,
                                     Map<String, String> queryParams, ExecutionContext context) {
        
        QueryBuilder.DeleteQuery deleteQuery = queryBuilder.buildDeleteQuery(table, queryParams);
        String sql = deleteQuery.getSql();
        List<Object> parameters = deleteQuery.getParameters();
        
        context.getLogger().info("DELETEクエリを実行: " + sql);
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            setParameters(pstmt, parameters);
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
    
    /**
     * パラメータをPreparedStatementに設定
     */
    private void setParameters(PreparedStatement pstmt, List<Object> parameters) throws SQLException {
        for (int i = 0; i < parameters.size(); i++) {
            Object param = parameters.get(i);
            pstmt.setObject(i + 1, param);
        }
    }
    
    /**
     * ResultSetをList<Map>に変換
     */
    private List<Map<String, Object>> convertResultSetToList(ResultSet rs) throws SQLException {
        List<Map<String, Object>> resultList = new ArrayList<>();
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        
        while (rs.next()) {
            Map<String, Object> row = new HashMap<>();
            for (int i = 1; i <= columns; ++i) {
                String columnName = md.getColumnName(i);
                Object value = rs.getObject(i);
                row.put(columnName, value);
            }
            resultList.add(row);
        }
        
        return resultList;
    }
}
