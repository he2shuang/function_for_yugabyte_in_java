package com.yugabyte.v3.service;

import com.microsoft.azure.functions.ExecutionContext;
import com.yugabyte.v3.exception.DatabaseException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * データベースメタデータサービスV3
 * エラー処理とキャッシュメカニズムを強化
 */
public class DatabaseMetadataServiceV3 {
    
    /**
     * テーブルの列メタデータをキャッシュ、テーブル名をキーとする。
     */
    private static final Map<String, Map<String, ColumnMetadata>> tableMetadataCache = new ConcurrentHashMap<>();
    
    /**
     * テーブルの主キー列名をキャッシュ、テーブル名をキーとする。
     */
    private static final Map<String, String> primaryKeyCache = new ConcurrentHashMap<>();
    
    /**
     * タイムスタンプ型名セット
     */
    private static final Set<String> TIMESTAMP_TYPE_NAMES = new HashSet<>(Arrays.asList(
            "timestamp",          // PostgreSQL, Oracle, etc.
            "timestamptz",        // PostgreSQL (timestamp with time zone)
            "datetime",           // MySQL, SQL Server
            "timestamp without time zone", // JDBC standard
            "timestamp with time zone"     // JDBC standard
    ));
    
    /**
     * 列メタデータレコード
     */
    public record ColumnMetadata(String typeName, boolean isNullable) {}
    
    /**
     * 指定されたテーブルがデータベースに存在するかどうかを確認。
     */
    public static boolean tableExists(Connection conn, String tableName, ExecutionContext context) {
        try {
            return !getTableColumns(conn, tableName, context).isEmpty();
        } catch (DatabaseException e) {
            // 如果getTableColumns抛出DatabaseException（如表不存在），则返回false
            context.getLogger().warning("テーブル存在確認中に例外が発生しました: " + e.getMessage());
            return false;
        }
    }
    
    /**
     * テーブルの主キー列名を取得
     */
    public static String getPrimaryKeyColumnName(Connection conn, String tableName, ExecutionContext context) {
        if (primaryKeyCache.containsKey(tableName)) {
            return primaryKeyCache.get(tableName);
        }
        
        context.getLogger().info("メタデータキャッシュミス: テーブル '" + tableName + "' の主キーをクエリ中。");
        
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getPrimaryKeys(null, null, tableName)) {
                if (rs.next()) {
                    String pkColumnName = rs.getString("COLUMN_NAME");
                    primaryKeyCache.put(tableName, pkColumnName);
                    return pkColumnName;
                }
            }
            
            // null結果をキャッシュし、主キーのないテーブルの重複クエリを回避
            primaryKeyCache.put(tableName, null);
            return null;
            
        } catch (SQLException e) {
            context.getLogger().severe("主キー列名の取得中にエラーが発生しました: " + e.getMessage());
            throw DatabaseException.queryFailed("getPrimaryKeys", e.getMessage(), e);
        }
    }
    
    /**
     * テーブルのすべての列とその属性を取得
     */
    public static Map<String, ColumnMetadata> getTableColumns(Connection conn, String tableName, ExecutionContext context) {
        if (tableMetadataCache.containsKey(tableName)) {
            return tableMetadataCache.get(tableName);
        }
        
        context.getLogger().info("メタデータキャッシュミス: テーブル '" + tableName + "' の列をクエリ中。");
        
        try {
            Map<String, ColumnMetadata> columnMap = new HashMap<>();
            DatabaseMetaData metaData = conn.getMetaData();
            
            try (ResultSet rs = metaData.getColumns(null, null, tableName, null)) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String typeName = rs.getString("TYPE_NAME");
                    boolean isNullable = "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE"));
                    columnMap.put(columnName, new ColumnMetadata(typeName, isNullable));
                }
            }
            
            if (columnMap.isEmpty()) {
                throw DatabaseException.tableNotFound(tableName);
            }
            
            tableMetadataCache.put(tableName, columnMap);
            return columnMap;
            
        } catch (SQLException e) {
            context.getLogger().severe("テーブル列情報の取得中にエラーが発生しました: " + e.getMessage());
            throw DatabaseException.queryFailed("getColumns", e.getMessage(), e);
        }
    }
    
    /**
     * テーブルの列メタデータ内でタイムスタンプ型の列を検索。
     */
    public static String findAutoTimestampColumn(Map<String, ColumnMetadata> tableColumns) {
        for (Map.Entry<String, ColumnMetadata> entry : tableColumns.entrySet()) {
            String columnName = entry.getKey();
            ColumnMetadata metadata = entry.getValue();
            
            if (isTimestampType(metadata)) {
                return columnName;
            }
        }
        
        return null;
    }
    
    /**
     * 列のメタデータがタイムスタンプ型を表すかどうかを確認。
     */
    private static boolean isTimestampType(ColumnMetadata metadata) {
        if (metadata == null || metadata.typeName() == null) {
            return false;
        }
        
        String typeNameInLower = metadata.typeName().toLowerCase();
        return TIMESTAMP_TYPE_NAMES.contains(typeNameInLower);
    }
    
    /**
     * キャッシュをクリア（テストやキャッシュの更新が必要なシナリオ用）
     */
    public static void clearCache() {
        tableMetadataCache.clear();
        primaryKeyCache.clear();
    }
    
    /**
     * 指定されたテーブルのキャッシュをクリア
     */
    public static void clearCacheForTable(String tableName) {
        tableMetadataCache.remove(tableName);
        primaryKeyCache.remove(tableName);
    }
}
