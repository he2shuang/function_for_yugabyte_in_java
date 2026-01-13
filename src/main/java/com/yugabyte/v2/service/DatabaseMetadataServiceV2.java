package com.yugabyte.v2.service;

import com.microsoft.azure.functions.ExecutionContext;
import com.yugabyte.v2.exception.DatabaseException;

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
 * 数据库元数据服务V2
 * 增强错误处理和缓存机制
 */
public class DatabaseMetadataServiceV2 {
    
    /**
     * 缓存表的列元数据，以表名为键。
     */
    private static final Map<String, Map<String, ColumnMetadata>> tableMetadataCache = new ConcurrentHashMap<>();
    
    /**
     * 缓存表的主键列名，以表名为键。
     */
    private static final Map<String, String> primaryKeyCache = new ConcurrentHashMap<>();
    
    /**
     * 时间戳类型名称集合
     */
    private static final Set<String> TIMESTAMP_TYPE_NAMES = new HashSet<>(Arrays.asList(
            "timestamp",          // PostgreSQL, Oracle, etc.
            "timestamptz",        // PostgreSQL (timestamp with time zone)
            "datetime",           // MySQL, SQL Server
            "timestamp without time zone", // JDBC standard
            "timestamp with time zone"     // JDBC standard
    ));
    
    /**
     * 列元数据记录
     */
    public record ColumnMetadata(String typeName, boolean isNullable) {}
    
    /**
     * 检查指定的表是否存在于数据库中。
     */
    public static boolean tableExists(Connection conn, String tableName, ExecutionContext context) throws SQLException {
        try {
            return !getTableColumns(conn, tableName, context).isEmpty();
        } catch (SQLException e) {
            context.getLogger().severe("检查表是否存在时发生错误: " + e.getMessage());
            throw DatabaseException.tableNotFound(tableName);
        }
    }
    
    /**
     * 获取表的主键列名
     */
    public static String getPrimaryKeyColumnName(Connection conn, String tableName, ExecutionContext context) throws SQLException {
        if (primaryKeyCache.containsKey(tableName)) {
            return primaryKeyCache.get(tableName);
        }
        
        context.getLogger().info("Metadata Cache Miss: Querying primary key for table '" + tableName + "'.");
        
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getPrimaryKeys(null, null, tableName)) {
                if (rs.next()) {
                    String pkColumnName = rs.getString("COLUMN_NAME");
                    primaryKeyCache.put(tableName, pkColumnName);
                    return pkColumnName;
                }
            }
            
            // 缓存null结果，以避免对没有主键的表进行重复查询
            primaryKeyCache.put(tableName, null);
            return null;
            
        } catch (SQLException e) {
            context.getLogger().severe("获取主键列名时发生错误: " + e.getMessage());
            throw DatabaseException.queryFailed("getPrimaryKeys", e.getMessage());
        }
    }
    
    /**
     * 获取表的所有列及其属性
     */
    public static Map<String, ColumnMetadata> getTableColumns(Connection conn, String tableName, ExecutionContext context) throws SQLException {
        if (tableMetadataCache.containsKey(tableName)) {
            return tableMetadataCache.get(tableName);
        }
        
        context.getLogger().info("Metadata Cache Miss: Querying columns for table '" + tableName + "'.");
        
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
            context.getLogger().severe("获取表列信息时发生错误: " + e.getMessage());
            throw DatabaseException.queryFailed("getColumns", e.getMessage());
        }
    }
    
    /**
     * 在表的列元数据中查找一个时间戳类型的列。
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
     * 检查列的元数据是否表示一个时间戳类型。
     */
    private static boolean isTimestampType(ColumnMetadata metadata) {
        if (metadata == null || metadata.typeName() == null) {
            return false;
        }
        
        String typeNameInLower = metadata.typeName().toLowerCase();
        return TIMESTAMP_TYPE_NAMES.contains(typeNameInLower);
    }
    
    /**
     * 清除缓存（用于测试或需要刷新缓存的场景）
     */
    public static void clearCache() {
        tableMetadataCache.clear();
        primaryKeyCache.clear();
    }
    
    /**
     * 清除指定表的缓存
     */
    public static void clearCacheForTable(String tableName) {
        tableMetadataCache.remove(tableName);
        primaryKeyCache.remove(tableName);
    }
}
