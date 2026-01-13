package com.yugabyte.v2.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.yugabyte.v2.dto.ErrorResponse;
import com.yugabyte.v2.exception.BusinessException;
import com.yugabyte.v2.exception.ErrorCode;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * 响应工具类
 * 用于构建标准化的HTTP响应
 */
public class ResponseUtil {
    
    private static final Gson gson = new GsonBuilder()
        .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        .create();
    
    /**
     * 构建成功响应
     */
    public static HttpResponseMessage success(HttpRequestMessage<?> request, Object data) {
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("data", data);
        response.put("timestamp", System.currentTimeMillis());
        
        return request.createResponseBuilder(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body(gson.toJson(response))
                .build();
    }
    
    /**
     * 构建错误响应
     */
    public static HttpResponseMessage error(HttpRequestMessage<?> request, HttpStatus status, ErrorResponse error) {
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
     * 构建业务异常响应
     */
    public static HttpResponseMessage businessError(HttpRequestMessage<?> request, BusinessException exception) {
        return error(request, HttpStatus.BAD_REQUEST, exception.toErrorResponse());
    }
    
    /**
     * 构建验证异常响应（使用ErrorCode）
     */
    public static HttpResponseMessage validationError(HttpRequestMessage<?> request, ErrorCode errorCode, String errorName, String errorDetail) {
        ErrorResponse error = new ErrorResponse(errorCode.getCode(), errorName != null ? errorName : "-", errorDetail);
        return error(request, HttpStatus.BAD_REQUEST, error);
    }
    
    /**
     * 构建数据库异常响应（使用ErrorCode）
     */
    public static HttpResponseMessage databaseError(HttpRequestMessage<?> request, ErrorCode errorCode, String errorName, String errorDetail) {
        ErrorResponse error = new ErrorResponse(errorCode.getCode(), errorName != null ? errorName : "-", errorDetail);
        return error(request, HttpStatus.INTERNAL_SERVER_ERROR, error);
    }
    
    /**
     * 构建未捕获异常响应
     */
    public static HttpResponseMessage internalError(HttpRequestMessage<?> request, Exception exception) {
        ErrorResponse error = new ErrorResponse(
            ErrorCode.INTERNAL_ERROR.getCode(),
            "system",
            "系统内部错误，请联系管理员"
        );
        
        // 记录日志但不暴露给客户端
        return error(request, HttpStatus.INTERNAL_SERVER_ERROR, error);
    }
    
    /**
     * 构建配置错误响应
     */
    public static HttpResponseMessage configError(HttpRequestMessage<?> request, ErrorCode errorCode, String errorName, String errorDetail) {
        ErrorResponse error = new ErrorResponse(errorCode.getCode(), errorName != null ? errorName : "-", errorDetail);
        return error(request, HttpStatus.INTERNAL_SERVER_ERROR, error);
    }
}
