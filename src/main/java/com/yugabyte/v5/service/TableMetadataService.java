package com.yugabyte.v5.service;

import com.yugabyte.v5.exception.DatabaseException;
import com.yugabyte.v5.repository.DatabaseMetadataRepository;

import java.sql.Connection;
import java.util.Map;

/**
 * テーブルメタデータサービス
 * テーブルのメタデータ管理を担当
 */
public class TableMetadataService {
    
    private final DatabaseMetadataRepository metadataRepository;
    
    public TableMetadataService() {
        this.metadataRepository = new DatabaseMetadataRepository();
    }
    
    /**
     * テーブルのメタデータを取得
     */
    public Map<String, String> getTableMetadata(Connection connection, String table) {
        // 1. テーブルが存在するか確認
        if (!metadataRepository.tableExists(connection, table)) {
            throw DatabaseException.tableNotFound(table);
        }
        
        // 2. 列メタデータを取得
        Map<String, String> columnTypes = metadataRepository.getColumnTypes(connection, table);
        
        if (columnTypes == null || columnTypes.isEmpty()) {
            throw DatabaseException.queryFailed("metadata", 
                String.format("テーブル '%s' のメタデータを取得できませんでした", table));
        }
        
        return columnTypes;
    }
    
    /**
     * テーブルの主キーを取得
     */
    public String getPrimaryKey(Connection connection, String table) {
        return metadataRepository.getPrimaryKey(connection, table);
    }
}
