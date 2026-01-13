package com.yugabyte.v5.repository;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * データベースメタデータリポジトリ
 * データベースメタデータの取得を担当
 */
public class DatabaseMetadataRepository {
    
    /**
     * テーブルが存在するか確認
     */
    public boolean tableExists(Connection connection, String tableName) {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet tables = metaData.getTables(null, null, tableName, new String[]{"TABLE"})) {
                return tables.next();
            }
        } catch (SQLException e) {
            throw new RuntimeException("テーブル存在確認中にエラーが発生しました: " + e.getMessage(), e);
        }
    }
    
    /**
     * テーブルの列タイプを取得
     */
    public Map<String, String> getColumnTypes(Connection connection, String tableName) {
        Map<String, String> columnTypes = new HashMap<>();
        
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet columns = metaData.getColumns(null, null, tableName, null)) {
                while (columns.next()) {
                    String columnName = columns.getString("COLUMN_NAME");
                    String typeName = columns.getString("TYPE_NAME");
                    columnTypes.put(columnName, typeName);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("列メタデータ取得中にエラーが発生しました: " + e.getMessage(), e);
        }
        
        return columnTypes;
    }
    
    /**
     * テーブルの主キーを取得
     */
    public String getPrimaryKey(Connection connection, String tableName) {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, tableName)) {
                if (primaryKeys.next()) {
                    return primaryKeys.getString("COLUMN_NAME");
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("主キー取得中にエラーが発生しました: " + e.getMessage(), e);
        }
        
        return null;
    }
    
    /**
     * テーブルのすべての列名を取得
     */
    public String[] getColumnNames(Connection connection, String tableName) {
        Map<String, String> columnTypes = getColumnTypes(connection, tableName);
        return columnTypes.keySet().toArray(new String[0]);
    }
}
