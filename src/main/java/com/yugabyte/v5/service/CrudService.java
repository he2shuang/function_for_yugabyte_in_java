package com.yugabyte.v5.service;

import com.microsoft.azure.functions.ExecutionContext;
import com.yugabyte.v5.exception.DatabaseException;
import com.yugabyte.v5.exception.ValidationException;
import com.yugabyte.v5.repository.TableRepository;
import com.yugabyte.v5.service.DataValidationService;
import com.yugabyte.v5.service.TableMetadataService;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * CRUDサービス
 * ビジネスロジック層：ビジネスルールの検証とロジックを担当
 */
public class CrudService {
    
    private final TableRepository tableRepository;
    private final TableMetadataService tableMetadataService;
    private final DataValidationService dataValidationService;
    
    public CrudService() {
        this.tableRepository = new TableRepository();
        this.tableMetadataService = new TableMetadataService();
        this.dataValidationService = new DataValidationService();
    }
    
    /**
     * レコードを作成
     */
    public Map<String, Object> create(Connection connection, String table, 
                                     Map<String, Object> data, ExecutionContext context) {
        context.getLogger().info("Creating record in table: " + table);
        
        // 1. テーブルメタデータを取得
        Map<String, String> columnTypes = tableMetadataService.getTableMetadata(connection, table);
        
        // 2. データを検証
        dataValidationService.validateData(data, columnTypes, table);
        
        // 3. リポジトリに処理を委譲
        return tableRepository.insert(connection, table, data, columnTypes);
    }
    
    /**
     * レコードを読み取り
     */
    public List<Map<String, Object>> read(Connection connection, String table,
                                         Map<String, String> queryParams, ExecutionContext context) {
        context.getLogger().info("Reading records from table: " + table);
        
        // 1. テーブルメタデータを取得
        Map<String, String> columnTypes = tableMetadataService.getTableMetadata(connection, table);
        
        // 2. クエリパラメータを検証
        dataValidationService.validateQueryParameters(queryParams, columnTypes, table);
        
        // 3. リポジトリに処理を委譲
        return tableRepository.select(connection, table, queryParams, columnTypes);
    }
    
    /**
     * レコードを更新
     */
    public Map<String, Object> update(Connection connection, String table,
                                     Map<String, Object> data, Map<String, String> queryParams,
                                     ExecutionContext context) {
        context.getLogger().info("Updating records in table: " + table);
        
        // 1. テーブルメタデータを取得
        Map<String, String> columnTypes = tableMetadataService.getTableMetadata(connection, table);
        
        // 2. データとクエリパラメータを検証
        dataValidationService.validateData(data, columnTypes, table);
        dataValidationService.validateQueryParameters(queryParams, columnTypes, table);
        
        // 3. リポジトリに処理を委譲
        return tableRepository.update(connection, table, data, queryParams, columnTypes);
    }
    
    /**
     * レコードを削除
     */
    public Map<String, Object> delete(Connection connection, String table,
                                     Map<String, String> queryParams, ExecutionContext context) {
        context.getLogger().info("Deleting records from table: " + table);
        
        // 1. テーブルメタデータを取得
        Map<String, String> columnTypes = tableMetadataService.getTableMetadata(connection, table);
        
        // 2. クエリパラメータを検証
        dataValidationService.validateQueryParameters(queryParams, columnTypes, table);
        
        // 3. リポジトリに処理を委譲
        return tableRepository.delete(connection, table, queryParams, columnTypes);
    }
}
