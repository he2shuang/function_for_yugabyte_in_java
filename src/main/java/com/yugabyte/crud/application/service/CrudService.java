package com.yugabyte.crud.application.service;

import com.microsoft.azure.functions.ExecutionContext;
import com.yugabyte.crud.domain.exception.DatabaseException;
import com.yugabyte.crud.domain.exception.ValidationException;
import com.yugabyte.crud.domain.repository.TableRepository;
import com.yugabyte.crud.domain.service.DataValidationService;
import com.yugabyte.crud.domain.service.TableMetadataService;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * CRUDサービス
 * アプリケーションサービス層：ビジネスロジックの調整を担当
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
        
        // 1. テーブルが存在するか確認
        tableMetadataService.validateTableExists(connection, table, context);
        
        // 2. データを検証
        dataValidationService.validateForInsert(connection, table, data, context);
        
        // 3. レコードを作成
        return tableRepository.insert(connection, table, data, context);
    }
    
    /**
     * レコードを読み取り
     */
    public List<Map<String, Object>> read(Connection connection, String table,
                                         Map<String, String> queryParams, ExecutionContext context) {
        
        // 1. テーブルが存在するか確認
        tableMetadataService.validateTableExists(connection, table, context);
        
        // 2. クエリパラメータを検証
        if (queryParams != null && !queryParams.isEmpty()) {
            dataValidationService.validateQueryParameters(connection, table, queryParams, context);
        }
        
        // 3. レコードを検索
        return tableRepository.select(connection, table, queryParams, context);
    }
    
    /**
     * レコードを更新
     */
    public Map<String, Object> update(Connection connection, String table,
                                     Map<String, Object> data, Map<String, String> queryParams,
                                     ExecutionContext context) {
        
        // 1. テーブルが存在するか確認
        tableMetadataService.validateTableExists(connection, table, context);
        
        // 2. データとクエリパラメータを検証
        dataValidationService.validateForUpdate(connection, table, data, queryParams, context);
        
        // 3. レコードを更新
        return tableRepository.update(connection, table, data, queryParams, context);
    }
    
    /**
     * レコードを削除
     */
    public Map<String, Object> delete(Connection connection, String table,
                                     Map<String, String> queryParams, ExecutionContext context) {
        
        // 1. テーブルが存在するか確認
        tableMetadataService.validateTableExists(connection, table, context);
        
        // 2. クエリパラメータを検証
        dataValidationService.validateQueryParameters(connection, table, queryParams, context);
        
        // 3. レコードを削除
        return tableRepository.delete(connection, table, queryParams, context);
    }
}
