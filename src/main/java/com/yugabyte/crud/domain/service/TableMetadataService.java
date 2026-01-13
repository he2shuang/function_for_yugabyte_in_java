package com.yugabyte.crud.domain.service;

import com.microsoft.azure.functions.ExecutionContext;
import com.yugabyte.crud.domain.exception.DatabaseException;
import com.yugabyte.crud.domain.repository.DatabaseMetadataRepository;
import com.yugabyte.crud.domain.repository.DatabaseMetadataRepository.ColumnMetadata;

import java.sql.Connection;
import java.util.Map;

/**
 * テーブルメタデータサービス
 * テーブルメタデータ関連のビジネスロジックを担当
 */
public class TableMetadataService {
    
    private final DatabaseMetadataRepository metadataRepository;
    
    public TableMetadataService() {
        this.metadataRepository = new DatabaseMetadataRepository();
    }
    
    /**
     * テーブルが存在するか検証
     */
    public void validateTableExists(Connection connection, String tableName, ExecutionContext context) {
        if (!metadataRepository.tableExists(connection, tableName, context)) {
            throw DatabaseException.tableNotFound(tableName);
        }
    }
    
    /**
     * テーブルの列メタデータを取得
     */
    public Map<String, ColumnMetadata> getTableColumns(Connection connection, String tableName, ExecutionContext context) {
        return metadataRepository.getTableColumns(connection, tableName, context);
    }
    
    /**
     * テーブルの主キー列名を取得
     */
    public String getPrimaryKeyColumnName(Connection connection, String tableName, ExecutionContext context) {
        return metadataRepository.getPrimaryKeyColumnName(connection, tableName, context);
    }
    
    /**
     * 自動タイムスタンプ列を検索
     */
    public String findAutoTimestampColumn(Map<String, ColumnMetadata> tableColumns) {
        return metadataRepository.findAutoTimestampColumn(tableColumns);
    }
    
    /**
     * キャッシュをクリア
     */
    public void clearCache() {
        metadataRepository.clearCache();
    }
    
    /**
     * 指定されたテーブルのキャッシュをクリア
     */
    public void clearCacheForTable(String tableName) {
        metadataRepository.clearCacheForTable(tableName);
    }
}
