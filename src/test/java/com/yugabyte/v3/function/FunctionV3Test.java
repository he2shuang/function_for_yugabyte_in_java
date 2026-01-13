package com.yugabyte.v3.function;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.yugabyte.v3.exception.DatabaseException;
import com.yugabyte.v3.exception.ErrorCode;
import com.yugabyte.v3.exception.ValidationException;
import com.yugabyte.v3.handler.CrudHandlerV3;
import com.yugabyte.v3.service.DatabaseMetadataServiceV3;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Optional;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * FunctionV3 测试类
 */
class FunctionV3Test {
    
    @Mock
    private HttpRequestMessage<Optional<String>> request;
    
    @Mock
    private ExecutionContext context;
    
    @Mock
    private Connection connection;
    
    @Mock
    private CrudHandlerV3 crudHandler;
    
    private FunctionV3 function;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        function = new FunctionV3();
        
        // 设置环境变量
        System.setProperty("DB_URL", "jdbc:yugabyte://localhost:5433/yugabyte");
        System.setProperty("DB_USER", "yugabyte");
        System.setProperty("DB_PASSWORD", "yugabyte");
        
        // 模拟Logger
        Logger logger = mock(Logger.class);
        when(context.getLogger()).thenReturn(logger);
    }
    
    @Test
    void testMissingEnvironmentVariables() {
        // 清除环境变量
        System.clearProperty("DB_URL");
        System.clearProperty("DB_USER");
        System.clearProperty("DB_PASSWORD");
        
        when(request.getHttpMethod()).thenReturn(HttpMethod.GET);
        when(request.getQueryParameters()).thenReturn(new HashMap<>());
        
        HttpResponseMessage response = function.run(request, "test_table", context);
        
        assertNotNull(response);
        assertEquals(500, response.getStatus().value()); // INTERNAL_SERVER_ERROR
    }
    
    @Test
    void testTableNotFound() throws SQLException {
        // 恢复环境变量
        System.setProperty("DB_URL", "jdbc:yugabyte://localhost:5433/yugabyte");
        System.setProperty("DB_USER", "yugabyte");
        System.setProperty("DB_PASSWORD", "yugabyte");
        
        when(request.getHttpMethod()).thenReturn(HttpMethod.GET);
        when(request.getQueryParameters()).thenReturn(new HashMap<>());
        
        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class);
             MockedStatic<DatabaseMetadataServiceV3> serviceMock = mockStatic(DatabaseMetadataServiceV3.class)) {
            
            driverManagerMock.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
                .thenReturn(connection);
            
            serviceMock.when(() -> DatabaseMetadataServiceV3.tableExists(any(Connection.class), anyString(), any(ExecutionContext.class)))
                .thenReturn(false);
            
            HttpResponseMessage response = function.run(request, "non_existent_table", context);
            
            assertNotNull(response);
            assertEquals(404, response.getStatus().value()); // NOT_FOUND
        }
    }
    
    @Test
    void testValidationExceptionHandling() throws SQLException {
        when(request.getHttpMethod()).thenReturn(HttpMethod.GET);
        when(request.getQueryParameters()).thenReturn(new HashMap<>());
        
        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class);
             MockedStatic<DatabaseMetadataServiceV3> serviceMock = mockStatic(DatabaseMetadataServiceV3.class)) {
            
            driverManagerMock.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
                .thenReturn(connection);
            
            serviceMock.when(() -> DatabaseMetadataServiceV3.tableExists(any(Connection.class), anyString(), any(ExecutionContext.class)))
                .thenReturn(true);
            
            // 直接模拟CrudHandlerV3的行为
            CrudHandlerV3 mockHandler = mock(CrudHandlerV3.class);
            when(mockHandler.handleGet(any(HttpRequestMessage.class), any(Connection.class), anyString(), any(ExecutionContext.class)))
                .thenThrow(new ValidationException(ErrorCode.MISSING_BODY, "request_body", "请求体不能为空"));
            
            // 使用反射设置FunctionV3中的handler
            java.lang.reflect.Field handlerField = FunctionV3.class.getDeclaredField("crudHandler");
            handlerField.setAccessible(true);
            handlerField.set(function, mockHandler);
            
            HttpResponseMessage response = function.run(request, "test_table", context);
            
            assertNotNull(response);
            assertEquals(400, response.getStatus().value()); // BAD_REQUEST
        } catch (NoSuchFieldException | IllegalAccessException e) {
            fail("反射设置handler失败: " + e.getMessage());
        }
    }
    
    @Test
    void testDatabaseExceptionHandling() throws SQLException {
        when(request.getHttpMethod()).thenReturn(HttpMethod.GET);
        when(request.getQueryParameters()).thenReturn(new HashMap<>());
        
        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class);
             MockedStatic<DatabaseMetadataServiceV3> serviceMock = mockStatic(DatabaseMetadataServiceV3.class)) {
            
            driverManagerMock.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
                .thenReturn(connection);
            
            serviceMock.when(() -> DatabaseMetadataServiceV3.tableExists(any(Connection.class), anyString(), any(ExecutionContext.class)))
                .thenReturn(true);
            
            // 直接模拟CrudHandlerV3的行为
            CrudHandlerV3 mockHandler = mock(CrudHandlerV3.class);
            when(mockHandler.handleGet(any(HttpRequestMessage.class), any(Connection.class), anyString(), any(ExecutionContext.class)))
                .thenThrow(DatabaseException.queryFailed("SELECT", "查询失败", new SQLException("连接超时")));
            
            // 使用反射设置FunctionV3中的handler
            java.lang.reflect.Field handlerField = FunctionV3.class.getDeclaredField("crudHandler");
            handlerField.setAccessible(true);
            handlerField.set(function, mockHandler);
            
            HttpResponseMessage response = function.run(request, "test_table", context);
            
            assertNotNull(response);
            // 超时异常应该返回504
            assertEquals(504, response.getStatus().value()); // GATEWAY_TIMEOUT
        } catch (NoSuchFieldException | IllegalAccessException e) {
            fail("反射设置handler失败: " + e.getMessage());
        }
    }
    
    @Test
    void testUnsupportedMethod() throws SQLException {
        when(request.getHttpMethod()).thenReturn(HttpMethod.PUT); // 不支持的PUT方法
        when(request.getQueryParameters()).thenReturn(new HashMap<>());
        
        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class);
             MockedStatic<DatabaseMetadataServiceV3> serviceMock = mockStatic(DatabaseMetadataServiceV3.class)) {
            
            driverManagerMock.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
                .thenReturn(connection);
            
            serviceMock.when(() -> DatabaseMetadataServiceV3.tableExists(any(Connection.class), anyString(), any(ExecutionContext.class)))
                .thenReturn(true);
            
            HttpResponseMessage response = function.run(request, "test_table", context);
            
            assertNotNull(response);
            assertEquals(405, response.getStatus().value()); // METHOD_NOT_ALLOWED
        }
    }
}
