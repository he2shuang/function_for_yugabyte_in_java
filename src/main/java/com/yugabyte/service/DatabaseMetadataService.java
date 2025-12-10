package com.yugabyte.service;

import com.microsoft.azure.functions.ExecutionContext;
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
 * 提供用于查询和缓存数据库元数据的服务。
 */
public class DatabaseMetadataService {

    /**
     * 缓存表的列元数据，以表名为键。
     */
    private static final Map<String, Map<String, ColumnMetadata>> tableMetadataCache = new ConcurrentHashMap<>();

    /**
     * 缓存表的主键列名，以表名为键。
     */
    private static final Map<String, String> primaryKeyCache = new ConcurrentHashMap<>();

    /**
     * 一个数据记录 (record)，用于封装关于单个数据库列的核心元数据。
     *
     * @param typeName   列的 SQL 类型名称 (例如 "varchar", "jsonb", "uuid")。
     * @param isNullable 如果列允许为 NULL，则为 true；否则为 false。
     */
    public record ColumnMetadata(String typeName, boolean isNullable) {}

    /**
     * 检查指定的表是否存在于数据库中。
     * 
     * @param conn       活动的数据库连接。
     * @param tableName  要检查的表的名称。
     * @param context    Azure Function 的执行上下文，用于日志记录。
     * @return 如果表存在，则返回 {@code true}；否则返回 {@code false}。
     * @throws SQLException 如果发生数据库访问错误。
     */
    public static boolean tableExists(Connection conn, String tableName, ExecutionContext context) throws SQLException {
        return !getTableColumns(conn, tableName, context).isEmpty();
    }

    /**
     * 使用数据库元数据动态获取并缓存指定表的主键列名。
     * 注意: 此方法假设表具有单列主键。
     *
     * @param conn       活动的数据库连接。
     * @param tableName  要查询的表的名称。
     * @param context    Azure Function 的执行上下文，用于日志记录。
     * @return 主键列的名称。如果表不存在或没有定义主键，则返回 {@code null}。
     * @throws SQLException 如果发生数据库访问错误。
     */
    public static String getPrimaryKeyColumnName(Connection conn, String tableName, ExecutionContext context) throws SQLException {
        if (primaryKeyCache.containsKey(tableName)) {
            return primaryKeyCache.get(tableName);
        }

        context.getLogger().info("Metadata Cache Miss: Querying primary key for table '" + tableName + "'.");
        DatabaseMetaData metaData = conn.getMetaData();
        try (ResultSet rs = metaData.getPrimaryKeys(null, null, tableName)) {
            if (rs.next()) {
                String pkColumnName = rs.getString("COLUMN_NAME");
                primaryKeyCache.put(tableName, pkColumnName);
                return pkColumnName;
            }
        }
        // 缓存 null 结果，以避免对没有主键的表进行重复查询
        primaryKeyCache.put(tableName, null);
        return null;
    }

    /**
     * 使用数据库元数据动态获取并缓存指定表的所有列及其属性。
     *
     * @param conn       活动的数据库连接。
     * @param tableName  要查询的表的名称。
     * @param context    Azure Function 的执行上下文，用于日志记录。
     * @return 一个 {@code Map<String, ColumnMetadata>}，其中键是列名，值是包含类型和可空性信息的 {@code ColumnMetadata} 对象。
     *         如果表不存在，则返回一个空 Map。
     * @throws SQLException 如果发生数据库访问错误。
     */
    public static Map<String, ColumnMetadata> getTableColumns(Connection conn, String tableName, ExecutionContext context) throws SQLException {
        if (tableMetadataCache.containsKey(tableName)) {
            return tableMetadataCache.get(tableName);
        }

        context.getLogger().info("Metadata Cache Miss: Querying columns for table '" + tableName + "'.");
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
        tableMetadataCache.put(tableName, columnMap);
        return columnMap;
    }

        // 1. 定义一个包含所有可能的时间戳类型名称的集合 (这部分仍然需要)
    //    这是进行类型判断的核心。
    private static final Set<String> TIMESTAMP_TYPE_NAMES = new HashSet<>(Arrays.asList(
            "timestamp",          // PostgreSQL, Oracle, etc.
            "timestamptz",        // PostgreSQL (timestamp with time zone)
            "datetime",           // MySQL, SQL Server
            "timestamp without time zone", // JDBC standard
            "timestamp with time zone"     // JDBC standard
    ));

    /**
     * 在表的列元数据中查找一个时间戳类型的列。
     *
     * <p>此方法纯粹基于数据类型进行查找。它会遍历所有列，
     * 并返回它找到的<b>第一个</b>类型为时间戳的列的名称。</p>
     *
     * @param tableColumns 一个Map，键是列名，值是该列的 ColumnMetadata。
     * @return 找到的第一个时间戳列的名称；如果表中没有任何时间戳列，则返回 null。
     */
    public static String findAutoTimestampColumn(Map<String, ColumnMetadata> tableColumns) {
        // 遍历表的所有列
        for (Map.Entry<String, ColumnMetadata> entry : tableColumns.entrySet()) {
            String columnName = entry.getKey();
            ColumnMetadata metadata = entry.getValue();

            // 检查这一列的数据类型是否为时间戳
            if (isTimestampType(metadata)) {
                // 如果是，立即返回这个列的名称
                return columnName;
            }
        }

        // 如果循环结束都没有找到任何时间戳列，则返回 null
        return null;
    }

    /**
     * 辅助方法，用于检查列的元数据是否表示一个时间戳类型。
     * @param metadata 列的元数据
     * @return 如果是时间戳类型则返回 true，否则返回 false
     */
    private static boolean isTimestampType(ColumnMetadata metadata) {
        if (metadata == null || metadata.typeName() == null) {
            return false;
        }
        // 将数据库返回的类型名称转为小写，以便与我们的集合进行不区分大小写的比较
        String typeNameInLower = metadata.typeName().toLowerCase();
        return TIMESTAMP_TYPE_NAMES.contains(typeNameInLower);
    }

}