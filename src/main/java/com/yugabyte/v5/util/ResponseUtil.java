package com.yugabyte.v5.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.yugabyte.v5.exception.ErrorCode;

import java.util.HashMap;
import java.util.Map;

/**
 * レスポンスユーティリティクラス
 * 標準化されたHTTPレスポンスを構築するために使用
 */
public class ResponseUtil {
    
    private static final Gson gson = new GsonBuilder()
        .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        .create();
    
    /**
     * 成功レスポンスを構築 (200 OK)
     */
    public static HttpResponseMessage success(HttpRequestMessage<?> request, Object data) {
        return createSuccessResponse(request, HttpStatus.OK, data);
    }
    
    /**
     * 作成成功レスポンスを構築 (201 Created)
     */
    public static HttpResponseMessage created(HttpRequestMessage<?> request, Object data) {
        return createSuccessResponse(request, HttpStatus.CREATED, data);
    }
    
    /**
     * コンテンツなし成功レスポンスを構築 (204 No Content)
     */
    public static HttpResponseMessage noContent(HttpRequestMessage<?> request) {
        return createSuccessResponse(request, HttpStatus.NO_CONTENT, null);
    }
    
    /**
     * 成功レスポンスを構築 (共通メソッド)
     */
    private static HttpResponseMessage createSuccessResponse(HttpRequestMessage<?> request, 
                                                           HttpStatus status, Object data) {
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        if (data != null) {
            response.put("data", data);
        }
        response.put("timestamp", System.currentTimeMillis());
        
        return request.createResponseBuilder(status)
                .header("Content-Type", "application/json")
                .body(gson.toJson(response))
                .build();
    }
    
    /**
     * エラーレスポンスを構築
     */
    public static HttpResponseMessage error(HttpRequestMessage<?> request, HttpStatus status, 
                                          String errorCode, String errorName, String errorDetail) {
        Map<String, Object> error = new HashMap<>();
        error.put("errorCode", errorCode);
        error.put("errorName", errorName);
        error.put("errorDetail", errorDetail);
        
        Map<String, Object> response = new HashMap<>();
        response.put("success", false);
        response.put("error", error);
        response.put("timestamp", System.currentTimeMillis());
        
        return request.createResponseBuilder(status)
                .header("Content-Type", "application/json")
                .body(gson.toJson(response))
                .build();
    }
    
    /**
     * 検証例外レスポンスを構築
     */
    public static HttpResponseMessage validationError(HttpRequestMessage<?> request, 
                                                    ErrorCode errorCode, String errorName, String errorDetail) {
        return error(request, HttpStatus.BAD_REQUEST, 
                    errorCode.getCode(), errorName != null ? errorName : "-", errorDetail);
    }
    
    /**
     * データベース例外レスポンスを構築
     */
    public static HttpResponseMessage databaseError(HttpRequestMessage<?> request, 
                                                  ErrorCode errorCode, String errorName, String errorDetail) {
        HttpStatus status = errorCode.toString().equals("TableNotFound") ? 
                          HttpStatus.NOT_FOUND : HttpStatus.INTERNAL_SERVER_ERROR;
        return error(request, status, errorCode.getCode(), errorName != null ? errorName : "-", errorDetail);
    }
    
    /**
     * 未キャッチ例外レスポンスを構築
     */
    public static HttpResponseMessage internalError(HttpRequestMessage<?> request, Exception exception) {
        return error(request, HttpStatus.INTERNAL_SERVER_ERROR,
                    ErrorCode.INTERNAL_ERROR.getCode(), "system",
                    "システム内部エラー。管理者に連絡してください");
    }
    
    /**
     * メソッド不許可エラーレスポンスを構築 (405 Method Not Allowed)
     */
    public static HttpResponseMessage methodNotAllowed(HttpRequestMessage<?> request, String method) {
        return error(request, HttpStatus.METHOD_NOT_ALLOWED,
                    ErrorCode.METHOD_NOT_ALLOWED.getCode(), "http_method",
                    "メソッド '" + method + "' は許可されていません");
    }
    
    /**
     * ゲートウェイタイムアウトエラーレスポンスを構築 (504 Gateway Timeout)
     */
    public static HttpResponseMessage gatewayTimeout(HttpRequestMessage<?> request, String reason) {
        return error(request, HttpStatus.GATEWAY_TIMEOUT,
                    ErrorCode.GATEWAY_TIMEOUT.getCode(), "timeout",
                    "ゲートウェイタイムアウト: " + reason);
    }
    
    /**
     * サービス利用不可エラーレスポンスを構築 (503 Service Unavailable)
     */
    public static HttpResponseMessage serviceUnavailable(HttpRequestMessage<?> request, String reason) {
        return error(request, HttpStatus.SERVICE_UNAVAILABLE,
                    ErrorCode.SERVICE_UNAVAILABLE.getCode(), "service",
                    "サービス利用不可: " + reason);
    }
}
