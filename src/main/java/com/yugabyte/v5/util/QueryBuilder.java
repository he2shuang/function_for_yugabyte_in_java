package com.yugabyte.v5.util;

import java.util.Set;

/**
 * SQLクエリビルダー
 * 動的なSQLクエリを構築するために使用
 */
public class QueryBuilder {
    
    /**
     * INSERT SQLを構築
     */
    public static String buildInsertSql(String table, Set<String> columns) {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(table).append(" (");
        
        // 列名を追加
        boolean first = true;
        for (String column : columns) {
            if (!first) {
                sql.append(", ");
            }
            sql.append(column);
            first = false;
        }
        
        sql.append(") VALUES (");
        
        // プレースホルダーを追加
        first = true;
        for (int i = 0; i < columns.size(); i++) {
            if (!first) {
                sql.append(", ");
            }
            sql.append("?");
            first = false;
        }
        
        sql.append(")");
        return sql.toString();
    }
    
    /**
     * SELECT SQLを構築
     */
    public static String buildSelectSql(String table, Set<String> whereColumns) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ").append(table);
        
        if (whereColumns != null && !whereColumns.isEmpty()) {
            sql.append(" WHERE ");
            buildWhereClause(sql, whereColumns);
        }
        
        return sql.toString();
    }
    
    /**
     * UPDATE SQLを構築
     */
    public static String buildUpdateSql(String table, Set<String> setColumns, Set<String> whereColumns) {
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ").append(table).append(" SET ");
        
        // SET句を構築
        boolean first = true;
        for (String column : setColumns) {
            if (!first) {
                sql.append(", ");
            }
            sql.append(column).append(" = ?");
            first = false;
        }
        
        // WHERE句を構築
        if (whereColumns != null && !whereColumns.isEmpty()) {
            sql.append(" WHERE ");
            buildWhereClause(sql, whereColumns);
        }
        
        return sql.toString();
    }
    
    /**
     * DELETE SQLを構築
     */
    public static String buildDeleteSql(String table, Set<String> whereColumns) {
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(table);
        
        if (whereColumns != null && !whereColumns.isEmpty()) {
            sql.append(" WHERE ");
            buildWhereClause(sql, whereColumns);
        }
        
        return sql.toString();
    }
    
    /**
     * WHERE句を構築
     */
    private static void buildWhereClause(StringBuilder sql, Set<String> whereColumns) {
        boolean first = true;
        for (String column : whereColumns) {
            if (!first) {
                sql.append(" AND ");
            }
            sql.append(column).append(" = ?");
            first = false;
        }
    }
}
