package com.yugabyte.crud.api.validator;

import com.microsoft.azure.functions.HttpMethod;
import com.yugabyte.crud.domain.exception.ValidationException;
import com.yugabyte.crud.domain.exception.ErrorCode;

/**
 * APIリクエストバリデーター
 * Function層での基本的なリクエスト検証を担当
 */
public class ApiRequestValidator {
    
    /**
     * リクエストの基本的な検証を実行
     */
    public void validateRequest(String tableName, HttpMethod httpMethod) {
        validateTableName(tableName);
        validateHttpMethod(httpMethod);
    }
    
    /**
     * テーブル名の検証
     */
    private void validateTableName(String tableName) {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw ValidationException.invalidTableName("テーブル名は空にできません");
        }
        
        // 基本的なSQLインジェクション防止
        if (tableName.contains(";") || tableName.contains("--") || 
            tableName.contains("/*") || tableName.contains("*/")) {
            throw ValidationException.invalidTableName("無効なテーブル名です: " + tableName);
        }
    }
    
    /**
     * HTTPメソッドの検証
     */
    private void validateHttpMethod(HttpMethod httpMethod) {
        if (httpMethod == null) {
            throw ValidationException.invalidHttpMethod("HTTPメソッドは空にできません");
        }
        
        // サポートされているメソッドか確認
        switch (httpMethod) {
            case GET:
            case POST:
            case PATCH:
            case DELETE:
                // サポートされているメソッド
                break;
            default:
                throw ValidationException.invalidHttpMethod(
                    String.format("サポートされていないHTTPメソッドです: %s", httpMethod)
                );
        }
    }
}
