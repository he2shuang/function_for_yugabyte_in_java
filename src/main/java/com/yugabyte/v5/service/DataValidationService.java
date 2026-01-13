package com.yugabyte.v5.service;

import com.yugabyte.v5.exception.ValidationException;
import com.yugabyte.v5.util.DataTypeValidator;

import java.util.Map;

/**
 * データ検証サービス
 * ビジネスルールに基づくデータ検証を担当
 */
public class DataValidationService {
    
    /**
     * リクエストデータを検証
     */
    public void validateData(Map<String, Object> data, Map<String, String> columnTypes, String table) {
        if (data == null || data.isEmpty()) {
            throw ValidationException.noValidColumns();
        }
        
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String column = entry.getKey();
            Object value = entry.getValue();
            
            // 1. 列がテーブルに存在するか確認
            if (!columnTypes.containsKey(column)) {
                throw ValidationException.unknownColumn(column, table);
            }
            
            // 2. データ型を検証
            String expectedType = columnTypes.get(column);
            if (value != null) {
                DataTypeValidator.validateDataType(value, expectedType, column);
            }
        }
    }
    
    /**
     * クエリパラメータを検証
     */
    public void validateQueryParameters(Map<String, String> queryParams, 
                                       Map<String, String> columnTypes, String table) {
        if (queryParams == null || queryParams.isEmpty()) {
            throw ValidationException.noValidFilters();
        }
        
        for (Map.Entry<String, String> entry : queryParams.entrySet()) {
            String column = entry.getKey();
            String value = entry.getValue();
            
            // 1. 列がテーブルに存在するか確認
            if (!columnTypes.containsKey(column)) {
                throw ValidationException.unknownColumn(column, table);
            }
            
            // 2. 値が空でないことを確認
            if (value == null || value.trim().isEmpty()) {
                throw ValidationException.notNull(column);
            }
            
            // 3. データ型を検証
            String expectedType = columnTypes.get(column);
            DataTypeValidator.validateQueryParameter(value, expectedType, column);
        }
    }
}
