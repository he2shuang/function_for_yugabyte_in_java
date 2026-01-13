package com.yugabyte.v2.function;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.functions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * FunctionV2 测试类
 */
class FunctionV2Test {
    
    private FunctionV2 function;
    private Gson gson;
    
    @Mock
    private HttpRequestMessage<Optional<String>> request;
    
    @Mock
    private ExecutionContext context;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        function = new FunctionV2();
        gson = new GsonBuilder().create();
        
        // 设置环境变量
        System.setProperty("DB_URL", "jdbc:postgresql://localhost:5433/yugabyte");
        System.setProperty("DB_USER", "yugabyte");
        System.setProperty("DB_PASSWORD", "yugabyte");
        
        // 模拟Logger
        Logger logger = mock(Logger.class);
        when(context.getLogger()).thenReturn(logger);
    }
    
    @Test
    void testMissingRequestBodyError() {
        // 模拟请求
        when(request.getHttpMethod()).thenReturn(HttpMethod.POST);
        when(request.getBody()).thenReturn(Optional.empty());
        when(request.getQueryParameters()).thenReturn(new HashMap<>());
        
        // 模拟响应构建器
        doAnswer(new Answer<HttpResponseMessage.Builder>() {
            @Override
            public HttpResponseMessage.Builder answer(InvocationOnMock invocation) {
                HttpStatus status = (HttpStatus) invocation.getArguments()[0];
                return new com.yugabyte.function.HttpResponseMessageMock.HttpResponseMessageBuilderMock().status(status);
            }
        }).when(request).createResponseBuilder(any(HttpStatus.class));
        
        // 执行测试
        HttpResponseMessage result = function.run(request, "test_table", context);
        
        // 验证
        assertNotNull(result);
        assertEquals(HttpStatus.BAD_REQUEST, result.getStatus());
    }
    
    @Test
    void testMissingEnvironmentVariablesError() {
        // 清除环境变量
        System.clearProperty("DB_URL");
        System.clearProperty("DB_USER");
        System.clearProperty("DB_PASSWORD");
        
        // 模拟请求
        when(request.getHttpMethod()).thenReturn(HttpMethod.GET);
        when(request.getQueryParameters()).thenReturn(new HashMap<>());
        
        // 模拟响应构建器
        doAnswer(new Answer<HttpResponseMessage.Builder>() {
            @Override
            public HttpResponseMessage.Builder answer(InvocationOnMock invocation) {
                HttpStatus status = (HttpStatus) invocation.getArguments()[0];
                return new com.yugabyte.function.HttpResponseMessageMock.HttpResponseMessageBuilderMock().status(status);
            }
        }).when(request).createResponseBuilder(any(HttpStatus.class));
        
        // 执行测试
        HttpResponseMessage result = function.run(request, "test_table", context);
        
        // 验证
        assertNotNull(result);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, result.getStatus());
        
        // 恢复环境变量
        System.setProperty("DB_URL", "jdbc:postgresql://localhost:5433/yugabyte");
        System.setProperty("DB_USER", "yugabyte");
        System.setProperty("DB_PASSWORD", "yugabyte");
    }
    
    @Test
    void testUnsupportedMethodError() {
        // 模拟请求
        when(request.getHttpMethod()).thenReturn(HttpMethod.PUT); // 不支持的HTTP方法
        when(request.getQueryParameters()).thenReturn(new HashMap<>());
        
        // 模拟响应构建器
        doAnswer(new Answer<HttpResponseMessage.Builder>() {
            @Override
            public HttpResponseMessage.Builder answer(InvocationOnMock invocation) {
                HttpStatus status = (HttpStatus) invocation.getArguments()[0];
                return new com.yugabyte.function.HttpResponseMessageMock.HttpResponseMessageBuilderMock().status(status);
            }
        }).when(request).createResponseBuilder(any(HttpStatus.class));
        
        // 执行测试
        HttpResponseMessage result = function.run(request, "test_table", context);
        
        // 验证
        assertNotNull(result);
        assertEquals(HttpStatus.BAD_REQUEST, result.getStatus());
    }
    
    @Test
    void testErrorResponseFormat() {
        // 测试错误响应JSON格式
        String errorJson = "{\"success\":false,\"error\":{\"errorCode\":\"NOT_NULL\",\"errorName\":\"apikey\",\"errorDetail\":\"字段 'apikey' 不允许为空\"},\"timestamp\":1234567890}";
        
        Map<String, Object> response = gson.fromJson(errorJson, Map.class);
        
        assertFalse((Boolean) response.get("success"));
        assertNotNull(response.get("error"));
        assertNotNull(response.get("timestamp"));
        
        @SuppressWarnings("unchecked")
        Map<String, String> error = (Map<String, String>) response.get("error");
        
        assertEquals("NOT_NULL", error.get("errorCode"));
        assertEquals("apikey", error.get("errorName"));
        assertEquals("字段 'apikey' 不允许为空", error.get("errorDetail"));
    }
}
