package com.yugabyte.v3.function;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.yugabyte.v3.dto.ErrorResponse;
import com.yugabyte.v3.exception.BusinessException;
import com.yugabyte.v3.exception.DatabaseException;
import com.yugabyte.v3.exception.ErrorCode;
import com.yugabyte.v3.handler.CrudHandlerV3;
import com.yugabyte.v3.service.DatabaseMetadataServiceV3;
import com.yugabyte.v3.util.ResponseUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Azure Function V3 - メインエントリクラス
 * エラー処理とレスポンス標準化を強化
 */
public class FunctionV3 {
    
    private static final CrudHandlerV3 crudHandler = new CrudHandlerV3();
    
    /**
     * Azure Function V3 のメイン実行メソッド
     * 
     * 
        methods = {HttpMethod.GET, HttpMethod.POST, HttpMethod.PATCH, HttpMethod.DELETE},
     * 
     */
    @FunctionName("YugaCrudApiV3")
    public HttpResponseMessage run(
            @HttpTrigger(
                name = "req",
                authLevel = AuthorizationLevel.ANONYMOUS,
                methods = {HttpMethod.GET, HttpMethod.POST, HttpMethod.PATCH, HttpMethod.DELETE},
                route = "v3/{table}")
            HttpRequestMessage<Optional<String>> request,
            @BindingName("table") String table,
            final ExecutionContext context) {
        
        context.getLogger().info("Java HTTP trigger processed a " + request.getHttpMethod() + 
                                " request for table: " + table + " (V3)");
        
        // 環境変数からデータベース接続情報を取得
        String url = System.getenv("DB_URL");
        String user = System.getenv("DB_USER");
        String password = System.getenv("DB_PASSWORD");
        
        // 環境変数の検証
        if (url == null || user == null || password == null) {
            context.getLogger().severe("データベース接続環境変数が設定されていません");
            return ResponseUtil.configError(request, 
                ErrorCode.DB_CONFIG_MISSING, 
                "environment_variables", 
                "データベース接続設定が不足しています。DB_URL、DB_USER、DB_PASSWORD環境変数を確認してください");
        }
        
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            // 動的セキュリティチェック：ターゲットテーブルがデータベースに存在するか確認
            if (!DatabaseMetadataServiceV3.tableExists(conn, table, context)) {
                return ResponseUtil.error(request, HttpStatus.NOT_FOUND,
                    new ErrorResponse(
                        ErrorCode.TABLE_NOT_FOUND.getCode(),
                        table,
                        String.format("テーブル '%s' は存在しません", table)
                    ));
            }
            
            // HTTPメソッドに基づいて、リクエストとデータベース接続をCrudHandlerV3に委譲
            switch (request.getHttpMethod()) {
                case GET:
                    List<Map<String, Object>> getResult = crudHandler.handleGet(request, conn, table, context);
                    return ResponseUtil.success(request, getResult);
                case POST:
                    Map<String, Object> postResult = crudHandler.handlePost(request, conn, table, context);
                    return ResponseUtil.created(request, postResult);
                case PATCH:
                    Map<String, Object> patchResult = crudHandler.handlePatch(request, conn, table, context);
                    return ResponseUtil.success(request, patchResult);
                case DELETE:
                    Map<String, Object> deleteResult = crudHandler.handleDelete(request, conn, table, context);
                    // DELETE成功時は204 No Contentを返す
                    return ResponseUtil.noContent(request);
                default:
                    // リストされていないメソッドの場合は405 Method Not Allowedを返す
                    context.getLogger().warning("HTTP method: " + request.getHttpMethod());
                    return ResponseUtil.methodNotAllowed(request, request.getHttpMethod().toString());
            }
            
        } catch (com.yugabyte.v3.exception.ValidationException e) {
            // 検証例外処理 (400 Bad Request)
            context.getLogger().warning("検証例外: " + e.getErrorDetail());
            return ResponseUtil.validationError(request, e.getErrorCode(), e.getErrorName(), e.getErrorDetail());
            
        } catch (DatabaseException e) {
            // データベース例外処理
            context.getLogger().severe("データベース例外: " + e.getMessage());
            // タイムアウトエラーの場合は504 Gateway Timeoutを返す
            if (e.isTimeout()) {
                return ResponseUtil.gatewayTimeout(request, "データベース接続タイムアウト: " + e.getMessage());
            }
            
            // その他のデータベースエラー (500 Internal Server Error)
            return ResponseUtil.databaseError(request, e.getErrorCode(), e.getErrorName(), e.getErrorDetail());
            
        } catch (Exception e) {
            // その他の未キャッチ例外処理 (500 Internal Server Error)
            context.getLogger().severe("未キャッチ例外: " + e.getMessage());
            return ResponseUtil.internalError(request, e);
        }
    }
}
