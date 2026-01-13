package com.yugabyte.v5.function;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.yugabyte.v5.function.ApiValidator;
import com.yugabyte.v5.handler.CrudHandler;
import com.yugabyte.v5.handler.ExceptionHandler;
import com.yugabyte.v5.config.DatabaseConfig;

import java.sql.Connection;
import java.util.Optional;

/**
 * Azure Function V5 - メインエントリクラス
 * HTTPアダプター層：基本的なHTTPリクエスト/レスポンスの処理のみを行う
 */
public class CrudFunction {
    
    private final CrudHandler crudHandler;
    private final ExceptionHandler exceptionHandler;
    private final ApiValidator apiValidator;
    
    public CrudFunction() {
        this.crudHandler = new CrudHandler();
        this.exceptionHandler = new ExceptionHandler();
        this.apiValidator = new ApiValidator();
    }
    
    /**
     * Azure Function V5 のメイン実行メソッド
     */
    @FunctionName("YugaCrudApiV5")
    public HttpResponseMessage run(
            @HttpTrigger(
                name = "req",
                authLevel = AuthorizationLevel.ANONYMOUS,
                methods = {HttpMethod.GET, HttpMethod.POST, HttpMethod.PATCH, HttpMethod.DELETE},
                route = "v5/{table}")
            HttpRequestMessage<Optional<String>> request,
            @BindingName("table") String table,
            final ExecutionContext context) {
        
        context.getLogger().info("Java HTTP trigger processed a " + request.getHttpMethod() + 
                                " request for table: " + table + " (V5)");
        
        try {
            // 1. 基本的なリクエスト検証
            apiValidator.validateRequest(table, request.getHttpMethod());
            
            // 2. データベース接続を取得
            Connection connection = DatabaseConfig.getConnection();
            
            try {
                // 3. リクエストをHandler層に委譲
                return crudHandler.handleRequest(request, connection, table, context);
                
            } finally {
                // 4. データベース接続を閉じる
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            }
            
        } catch (Exception e) {
            // 5. 例外処理をExceptionHandlerに委譲
            return exceptionHandler.handleException(e, request, context);
        }
    }
}
