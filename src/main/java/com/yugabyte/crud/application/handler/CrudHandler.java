package com.yugabyte.crud.application.handler;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.yugabyte.crud.application.processor.RequestProcessor;
import com.yugabyte.crud.application.service.CrudService;
import com.yugabyte.crud.domain.exception.DatabaseException;
import com.yugabyte.crud.domain.exception.ValidationException;
import com.yugabyte.crud.infrastructure.util.ResponseUtil;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * CRUDハンドラー
 * アプリケーション層：処理フローの調整を担当
 */
public class CrudHandler {
    
    private final CrudService crudService;
    private final RequestProcessor requestProcessor;
    
    public CrudHandler() {
        this.crudService = new CrudService();
        this.requestProcessor = new RequestProcessor();
    }
    
    /**
     * リクエストを処理し、適切なレスポンスを返す
     */
    public HttpResponseMessage handleRequest(HttpRequestMessage<Optional<String>> request,
                                           Connection connection,
                                           String table,
                                           ExecutionContext context) {
        
        try {
            // HTTPメソッドに基づいて処理をルーティング
            switch (request.getHttpMethod()) {
                case GET:
                    return handleGetRequest(request, connection, table, context);
                case POST:
                    return handlePostRequest(request, connection, table, context);
                case PATCH:
                    return handlePatchRequest(request, connection, table, context);
                case DELETE:
                    return handleDeleteRequest(request, connection, table, context);
                default:
                    // このケースはApiRequestValidatorで既に検証済み
                    return ResponseUtil.methodNotAllowed(request, request.getHttpMethod().toString());
            }
            
        } catch (ValidationException | DatabaseException e) {
            // これらの例外は上位層で処理される
            throw e;
        } catch (Exception e) {
            // その他の例外はRuntimeExceptionとして再スロー
            throw new RuntimeException("リクエスト処理中にエラーが発生しました", e);
        }
    }
    
    /**
     * GETリクエストを処理
     */
    private HttpResponseMessage handleGetRequest(HttpRequestMessage<Optional<String>> request,
                                               Connection connection,
                                               String table,
                                               ExecutionContext context) {
        // 1. リクエストパラメータを解析
        Map<String, String> queryParams = request.getQueryParameters();
        
        // 2. サービス層に処理を委譲
        List<Map<String, Object>> result = crudService.read(
            connection, table, queryParams, context
        );
        
        // 3. レスポンスを構築
        return ResponseUtil.success(request, result);
    }
    
    /**
     * POSTリクエストを処理
     */
    private HttpResponseMessage handlePostRequest(HttpRequestMessage<Optional<String>> request,
                                                Connection connection,
                                                String table,
                                                ExecutionContext context) {
        // 1. リクエストボディを解析
        String jsonBody = request.getBody().orElse(null);
        Map<String, Object> data = requestProcessor.parseRequestBody(jsonBody, table);
        
        // 2. サービス層に処理を委譲
        Map<String, Object> result = crudService.create(
            connection, table, data, context
        );
        
        // 3. レスポンスを構築
        return ResponseUtil.created(request, result);
    }
    
    /**
     * PATCHリクエストを処理
     */
    private HttpResponseMessage handlePatchRequest(HttpRequestMessage<Optional<String>> request,
                                                 Connection connection,
                                                 String table,
                                                 ExecutionContext context) {
        // 1. リクエストボディとクエリパラメータを解析
        String jsonBody = request.getBody().orElse(null);
        Map<String, String> queryParams = request.getQueryParameters();
        
        Map<String, Object> data = requestProcessor.parseRequestBody(jsonBody, table);
        
        // 2. サービス層に処理を委譲
        Map<String, Object> result = crudService.update(
            connection, table, data, queryParams, context
        );
        
        // 3. レスポンスを構築
        return ResponseUtil.success(request, result);
    }
    
    /**
     * DELETEリクエストを処理
     */
    private HttpResponseMessage handleDeleteRequest(HttpRequestMessage<Optional<String>> request,
                                                  Connection connection,
                                                  String table,
                                                  ExecutionContext context) {
        // 1. クエリパラメータを解析
        Map<String, String> queryParams = request.getQueryParameters();
        
        // 2. サービス層に処理を委譲
        Map<String, Object> result = crudService.delete(
            connection, table, queryParams, context
        );
        
        // 3. レスポンスを構築（DELETE成功時は204 No Content）
        return ResponseUtil.noContent(request);
    }
}
