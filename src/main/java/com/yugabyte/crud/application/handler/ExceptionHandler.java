package com.yugabyte.crud.application.handler;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.yugabyte.crud.api.dto.ErrorResponse;
import com.yugabyte.crud.domain.exception.BusinessException;
import com.yugabyte.crud.domain.exception.DatabaseException;
import com.yugabyte.crud.domain.exception.ValidationException;
import com.yugabyte.crud.infrastructure.util.ResponseUtil;

/**
 * 例外ハンドラー
 * 統一された例外処理を担当
 */
public class ExceptionHandler {
    
    /**
     * 例外を処理し、適切なHTTPレスポンスを返す
     */
    public HttpResponseMessage handleException(Exception exception, 
                                             HttpRequestMessage<?> request,
                                             ExecutionContext context) {
        
        // ログ記録
        logException(exception, context);
        
        // 例外タイプに基づいて処理
        if (exception instanceof ValidationException) {
            return handleValidationException((ValidationException) exception, request);
        } else if (exception instanceof DatabaseException) {
            return handleDatabaseException((DatabaseException) exception, request);
        } else if (exception instanceof BusinessException) {
            return handleBusinessException((BusinessException) exception, request);
        } else {
            return handleGenericException(exception, request);
        }
    }
    
    /**
     * 検証例外の処理
     */
    private HttpResponseMessage handleValidationException(ValidationException exception, 
                                                         HttpRequestMessage<?> request) {
        ErrorResponse error = exception.toErrorResponse();
        return ResponseUtil.error(request, HttpStatus.BAD_REQUEST, error);
    }
    
    /**
     * データベース例外の処理
     */
    private HttpResponseMessage handleDatabaseException(DatabaseException exception,
                                                       HttpRequestMessage<?> request) {
        ErrorResponse error = exception.toErrorResponse();
        
        // タイムアウトエラーの場合は504 Gateway Timeoutを返す
        if (exception.isTimeout()) {
            return ResponseUtil.gatewayTimeout(request, exception.getErrorDetail());
        }
        
        // テーブルが見つからない場合は404 Not Found
        if (exception.getErrorCode().toString().equals("TableNotFound")) {
            return ResponseUtil.error(request, HttpStatus.NOT_FOUND, error);
        }
        
        // その他のデータベースエラーは500 Internal Server Error
        return ResponseUtil.error(request, HttpStatus.INTERNAL_SERVER_ERROR, error);
    }
    
    /**
     * ビジネス例外の処理
     */
    private HttpResponseMessage handleBusinessException(BusinessException exception,
                                                       HttpRequestMessage<?> request) {
        ErrorResponse error = exception.toErrorResponse();
        return ResponseUtil.error(request, HttpStatus.BAD_REQUEST, error);
    }
    
    /**
     * 一般的な例外の処理
     */
    private HttpResponseMessage handleGenericException(Exception exception,
                                                      HttpRequestMessage<?> request) {
        ErrorResponse error = new ErrorResponse(
            "InternalError",
            "system",
            "システム内部エラー。管理者に連絡してください"
        );
        
        return ResponseUtil.error(request, HttpStatus.INTERNAL_SERVER_ERROR, error);
    }
    
    /**
     * 例外をログに記録
     */
    private void logException(Exception exception, ExecutionContext context) {
        if (exception instanceof ValidationException) {
            context.getLogger().warning("検証例外: " + exception.getMessage());
        } else if (exception instanceof DatabaseException) {
            context.getLogger().severe("データベース例外: " + exception.getMessage());
        } else {
            context.getLogger().severe("未キャッチ例外: " + exception.getMessage());
            exception.printStackTrace();
        }
    }
}
