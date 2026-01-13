package com.yugabyte.crud.domain.service;

import com.microsoft.azure.functions.ExecutionContext;
import com.yugabyte.crud.domain.exception.ValidationException;
import com.yugabyte.crud.domain.repository.DatabaseMetadataRepository;
import com.yugabyte.crud.domain.repository.DatabaseMetadataRepository.ColumnMetadata;
import com.yugabyte.crud.infrastructure.util.DataTypeValidator;

import java.sql.Connection;
import java.util.Map;
import java.util.Set;

/**
 * データ検証サービス
 * データ検証関連のビジネスロジックを担当
 */
public class DataValidationService {
    
    private final TableMetadataService tableMetadataService;
    
    public DataValidationService() {
        this.tableMetadataService = new TableMetadataService();
    }
    
    /**
     * INSERT操作のためのデータ検証
     */
    public void validateForInsert(Connection connection, String table, 
                                 Map<String, Object> data, ExecutionContext context) {
        
        // 1. テーブルのメタデータを取得
        Map<String, ColumnMetadata> tableColumns = tableMetadataService.getTableColumns(connection, table, context);
        String primaryKeyColumn = tableMetadataService.getPrimaryKeyColumnName(connection, table, context);
        String timestampColumn = tableMetadataService.findAutoTimestampColumn(tableColumns);
        
        // 2. 必須フィールドの検証
        validateRequiredFieldsForInsert(data, tableColumns, primaryKeyColumn, timestampColumn, table);
        
        // 3. 列の存在確認
        validateColumnsExist(data.keySet(), tableColumns, primaryKeyColumn, timestampColumn, table, "リクエストボディ");
        
        // 4. データ型の検証
        validateDataTypes(data, tableColumns, primaryKeyColumn, timestampColumn, table);
        
        // 5. NOT NULL制約の検証
        validateNotNullFields(data, tableColumns, primaryKeyColumn, timestampColumn, table);
    }
    
    /**
     * UPDATE操作のためのデータ検証
     */
    public void validateForUpdate(Connection connection, String table,
                                 Map<String, Object> data, Map<String, String> queryParams,
                                 ExecutionContext context) {
        
        // 1. テーブルのメタデータを取得
        Map<String, ColumnMetadata> tableColumns = tableMetadataService.getTableColumns(connection, table, context);
        String primaryKeyColumn = tableMetadataService.getPrimaryKeyColumnName(connection, table, context);
        String timestampColumn = tableMetadataService.findAutoTimestampColumn(tableColumns);
        
        // 2. クエリパラメータの検証
        validateQueryParameters(connection, table, queryParams, context);
        
        // 3. データの検証
        if (data != null && !data.isEmpty()) {
            validateColumnsExist(data.keySet(), tableColumns, primaryKeyColumn, timestampColumn, table, "リクエストボディ");
            validateDataTypes(data, tableColumns, primaryKeyColumn, timestampColumn, table);
            validateNotNullFieldsForUpdate(data, tableColumns, table);
        }
    }
    
    /**
     * クエリパラメータの検証
     */
    public void validateQueryParameters(Connection connection, String table,
                                       Map<String, String> queryParams, ExecutionContext context) {
        
        if (queryParams == null || queryParams.isEmpty()) {
            throw ValidationException.missingFilter();
        }
        
        Map<String, ColumnMetadata> tableColumns = tableMetadataService.getTableColumns(connection, table, context);
        validateColumnsExist(queryParams.keySet(), tableColumns, null, null, table, "クエリパラメータ");
    }
    
    /**
     * 必須フィールドの検証（INSERT用）
     */
    private void validateRequiredFieldsForInsert(Map<String, Object> data,
                                                Map<String, ColumnMetadata> tableColumns,
                                                String primaryKeyColumn, String timestampColumn,
                                                String table) {
        
        for (Map.Entry<String, ColumnMetadata> entry : tableColumns.entrySet()) {
            String columnName = entry.getKey();
            ColumnMetadata metadata = entry.getValue();
            
            // 主キー、タイムスタンプ列、NULL可能な列はスキップ
            if (columnName.equals(primaryKeyColumn) || 
                columnName.equals(timestampColumn) || 
                metadata.isNullable()) {
                continue;
            }
            
            // 必須フィールドが提供されているか確認
            if (!data.containsKey(columnName) || data.get(columnName) == null) {
                throw ValidationException.missingRequiredField(columnName);
            }
        }
    }
    
    /**
     * 列の存在確認
     */
    private void validateColumnsExist(Set<String> columns,
                                     Map<String, ColumnMetadata> tableColumns,
                                     String primaryKeyColumn, String timestampColumn,
                                     String table, String source) {
        
        for (String columnName : columns) {
            // 主キーとタイムスタンプ列はスキップ
            if ((primaryKeyColumn != null && columnName.equals(primaryKeyColumn)) ||
                (timestampColumn != null && columnName.equals(timestampColumn))) {
                continue;
            }
            
            // 列がテーブルに存在するか確認
            if (!tableColumns.containsKey(columnName)) {
                throw ValidationException.unknownColumn(columnName, table);
            }
        }
    }
    
    /**
     * データ型の検証
     */
    private void validateDataTypes(Map<String, Object> data,
                                  Map<String, ColumnMetadata> tableColumns,
                                  String primaryKeyColumn, String timestampColumn,
                                  String table) {
        
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String columnName = entry.getKey();
            Object value = entry.getValue();
            
            // 主キーとタイムスタンプ列はスキップ
            if ((primaryKeyColumn != null && columnName.equals(primaryKeyColumn)) ||
                (timestampColumn != null && columnName.equals(timestampColumn))) {
                continue;
            }
            
            // 列がテーブルに存在する場合のみ検証
            if (tableColumns.containsKey(columnName)) {
                ColumnMetadata metadata = tableColumns.get(columnName);
                DataTypeValidator.validateDataType(columnName, metadata, value, table);
            }
        }
    }
    
    /**
     * NOT NULL制約の検証（INSERT用）
     */
    private void validateNotNullFields(Map<String, Object> data,
                                      Map<String, ColumnMetadata> tableColumns,
                                      String primaryKeyColumn, String timestampColumn,
                                      String table) {
        
        for (Map.Entry<String, ColumnMetadata> entry : tableColumns.entrySet()) {
            String columnName = entry.getKey();
            ColumnMetadata metadata = entry.getValue();
            
            // NULL可能な列、主キー、タイムスタンプ列はスキップ
            if (metadata.isNullable() || 
                columnName.equals(primaryKeyColumn) || 
                columnName.equals(timestampColumn)) {
                continue;
            }
            
            // データに列が含まれ、値がnullの場合
            if (data.containsKey(columnName) && data.get(columnName) == null) {
                throw ValidationException.notNull(columnName);
            }
        }
    }
    
    /**
     * NOT NULL制約の検証（UPDATE用）
     */
    private void validateNotNullFieldsForUpdate(Map<String, Object> data,
                                               Map<String, ColumnMetadata> tableColumns,
                                               String table) {
        
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String columnName = entry.getKey();
            Object value = entry.getValue();
            
            // 列がテーブルに存在し、NOT NULL制約がある場合
            if (tableColumns.containsKey(columnName)) {
                ColumnMetadata metadata = tableColumns.get(columnName);
                if (!metadata.isNullable() && value == null) {
                    throw ValidationException.notNull(columnName);
                }
            }
        }
    }
}
