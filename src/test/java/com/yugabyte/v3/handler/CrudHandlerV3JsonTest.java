package com.yugabyte.v3.handler;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.yugabyte.v3.exception.ValidationException;
import com.yugabyte.v3.service.DatabaseMetadataServiceV3;
import com.yugabyte.v3.service.DatabaseMetadataServiceV3.ColumnMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * CrudHandlerV3 JSON解析错误测试
 */
class CrudHandlerV3JsonTest {
    
    @Mock
    private HttpRequestMessage<Optional<String>> request;
    
    @Mock
    private ExecutionContext context;
    
    @Mock
    private Connection connection;
    
    private CrudHandlerV3 handler;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        handler = new CrudHandlerV3();
        
        // 模拟Logger
        Logger logger = mock(Logger.class);
        when(context.getLogger()).thenReturn(logger);
    }
    
    @Test
    void testHandlePostWithInvalidJsonMissingBrace() {
        // 模拟请求：缺少右大括号的JSON
        String invalidJson = "{\"name\": \"test\", \"value\": 123"; // 缺少 }
        
        when(request.getBody()).thenReturn(Optional.of(invalidJson));
        
        try (MockedStatic<DatabaseMetadataServiceV3> serviceMock = mockStatic(DatabaseMetadataServiceV3.class)) {
            // 模拟表存在
            Map<String, ColumnMetadata> tableColumns = new HashMap<>();
            tableColumns.put("name", new ColumnMetadata("varchar", true));
            tableColumns.put("value", new ColumnMetadata("integer", true));
            
            serviceMock.when(() -> DatabaseMetadataServiceV3.getTableColumns(any(Connection.class), anyString(), any(ExecutionContext.class)))
                .thenReturn(tableColumns);
            
            serviceMock.when(() -> DatabaseMetadataServiceV3.getPrimaryKeyColumnName(any(Connection.class), anyString(), any(ExecutionContext.class)))
                .thenReturn("id");
            
            serviceMock.when(() -> DatabaseMetadataServiceV3.findAutoTimestampColumn(any(Map.class)))
                .thenReturn(null);
            
            // 应该抛出ValidationException
            ValidationException exception = assertThrows(ValidationException.class, () -> 
                handler.handlePost(request, connection, "test_table", context)
            );
            
            assertEquals("InvalidFormat", exception.getErrorCode().getCode());
            assertEquals("request_body", exception.getErrorName());
            assertTrue(exception.getErrorDetail().contains("无效的JSON格式"));
        }
    }
    
    @Test
    void testHandlePostWithInvalidJsonMissingComma() {
        // 模拟请求：缺少逗号的JSON
        String invalidJson = "{\"name\": \"test\" \"value\": 123}"; // 缺少逗号
        
        when(request.getBody()).thenReturn(Optional.of(invalidJson));
        
        try (MockedStatic<DatabaseMetadataServiceV3> serviceMock = mockStatic(DatabaseMetadataServiceV3.class)) {
            // 模拟表存在
            Map<String, ColumnMetadata> tableColumns = new HashMap<>();
            tableColumns.put("name", new ColumnMetadata("varchar", true));
            tableColumns.put("value", new ColumnMetadata("integer", true));
            
            serviceMock.when(() -> DatabaseMetadataServiceV3.getTableColumns(any(Connection.class), anyString(), any(ExecutionContext.class)))
                .thenReturn(tableColumns);
            
            serviceMock.when(() -> DatabaseMetadataServiceV3.getPrimaryKeyColumnName(any(Connection.class), anyString(), any(ExecutionContext.class)))
                .thenReturn("id");
            
            serviceMock.when(() -> DatabaseMetadataServiceV3.findAutoTimestampColumn(any(Map.class)))
                .thenReturn(null);
            
            // 应该抛出ValidationException
            ValidationException exception = assertThrows(ValidationException.class, () -> 
                handler.handlePost(request, connection, "test_table", context)
            );
            
            assertEquals("InvalidFormat", exception.getErrorCode().getCode());
            assertEquals("request_body", exception.getErrorName());
            assertTrue(exception.getErrorDetail().contains("无效的JSON格式"));
        }
    }
    
    @Test
    void testHandlePostWithInvalidJsonMissingQuote() {
        // 模拟请求：缺少引号的JSON
        String invalidJson = "{\"name\": test, \"value\": 123}"; // test缺少引号
        
        when(request.getBody()).thenReturn(Optional.of(invalidJson));
        
        try (MockedStatic<DatabaseMetadataServiceV3> serviceMock = mockStatic(DatabaseMetadataServiceV3.class)) {
            // 模拟表存在
            Map<String, ColumnMetadata> tableColumns = new HashMap<>();
            tableColumns.put("name", new ColumnMetadata("varchar", true));
            tableColumns.put("value", new ColumnMetadata("integer", true));
            
            serviceMock.when(() -> DatabaseMetadataServiceV3.getTableColumns(any(Connection.class), anyString(), any(ExecutionContext.class)))
                .thenReturn(tableColumns);
            
            serviceMock.when(() -> DatabaseMetadataServiceV3.getPrimaryKeyColumnName(any(Connection.class), anyString(), any(ExecutionContext.class)))
                .thenReturn("id");
            
            serviceMock.when(() -> DatabaseMetadataServiceV3.findAutoTimestampColumn(any(Map.class)))
                .thenReturn(null);
            
            // 应该抛出ValidationException
            ValidationException exception = assertThrows(ValidationException.class, () -> 
                handler.handlePost(request, connection, "test_table", context)
            );
            
            assertEquals("InvalidFormat", exception.getErrorCode().getCode());
            assertEquals("request_body", exception.getErrorName());
            assertTrue(exception.getErrorDetail().contains("无效的JSON格式"));
        }
    }
    
    @Test
    void testHandlePostWithEmptyJsonObject() {
        // 模拟请求：空JSON对象
        String validJson = "{}";
        
        when(request.getBody()).thenReturn(Optional.of(validJson));
        
        try (MockedStatic<DatabaseMetadataServiceV3> serviceMock = mockStatic(DatabaseMetadataServiceV3.class)) {
            // 模拟表存在
            Map<String, ColumnMetadata> tableColumns = new HashMap<>();
            tableColumns.put("id", new ColumnMetadata("uuid", false));
            tableColumns.put("name", new ColumnMetadata("varchar", false)); // 非空字段
            
            serviceMock.when(() -> DatabaseMetadataServiceV3.getTableColumns(any(Connection.class), anyString(), any(ExecutionContext.class)))
                .thenReturn(tableColumns);
            
            serviceMock.when(() -> DatabaseMetadataServiceV3.getPrimaryKeyColumnName(any(Connection.class), anyString(), any(ExecutionContext.class)))
                .thenReturn("id");
            
            serviceMock.when(() -> DatabaseMetadataServiceV3.findAutoTimestampColumn(any(Map.class)))
                .thenReturn(null);
            
            // 应该抛出ValidationException（缺少必需字段），但不是JSON格式错误
            ValidationException exception = assertThrows(ValidationException.class, () -> 
                handler.handlePost(request, connection, "test_table", context)
            );
            
            // 应该是MissingRequiredField，不是InvalidFormat
            assertEquals("MissingRequiredField", exception.getErrorCode().getCode());
        }
    }
    
    @Test
    void testHandlePatchWithInvalidJson() {
        // 模拟请求：无效的JSON
        String invalidJson = "{\"name\": \"test\""; // 缺少右大括号
        
        when(request.getBody()).thenReturn(Optional.of(invalidJson));
        when(request.getQueryParameters()).thenReturn(new HashMap<>());
        
        try (MockedStatic<DatabaseMetadataServiceV3> serviceMock = mockStatic(DatabaseMetadataServiceV3.class)) {
            // 模拟表存在
            Map<String, ColumnMetadata> tableColumns = new HashMap<>();
            tableColumns.put("name", new ColumnMetadata("varchar", true));
            
            serviceMock.when(() -> DatabaseMetadataServiceV3.getTableColumns(any(Connection.class), anyString(), any(ExecutionContext.class)))
                .thenReturn(tableColumns);
            
            serviceMock.when(() -> DatabaseMetadataServiceV3.getPrimaryKeyColumnName(any(Connection.class), anyString(), any(ExecutionContext.class)))
                .thenReturn("id");
            
            serviceMock.when(() -> DatabaseMetadataServiceV3.findAutoTimestampColumn(any(Map.class)))
                .thenReturn(null);
            
            // 应该抛出ValidationException
            ValidationException exception = assertThrows(ValidationException.class, () -> 
                handler.handlePatch(request, connection, "test_table", context)
            );
            
            assertEquals("InvalidFormat", exception.getErrorCode().getCode());
            assertEquals("request_body", exception.getErrorName());
            assertTrue(exception.getErrorDetail().contains("无效的JSON格式"));
        }
    }
    
    @Test
    void testHandlePostWithValidJson() {
        // 模拟请求：有效的JSON
        String validJson = "{\"name\": \"test\", \"value\": 123}";
        
        when(request.getBody()).thenReturn(Optional.of(validJson));
        
        try (MockedStatic<DatabaseMetadataServiceV3> serviceMock = mockStatic(DatabaseMetadataServiceV3.class)) {
            // 模拟表存在
            Map<String, ColumnMetadata> tableColumns = new HashMap<>();
            tableColumns.put("id", new ColumnMetadata("uuid", false));
            tableColumns.put("name", new ColumnMetadata("varchar", true));
            tableColumns.put("value", new ColumnMetadata("integer", true));
            
            serviceMock.when(() -> DatabaseMetadataServiceV3.getTableColumns(any(Connection.class), anyString(), any(ExecutionContext.class)))
                .thenReturn(tableColumns);
            
            serviceMock.when(() -> DatabaseMetadataServiceV3.getPrimaryKeyColumnName(any(Connection.class), anyString(), any(ExecutionContext.class)))
                .thenReturn("id");
            
            serviceMock.when(() -> DatabaseMetadataServiceV3.findAutoTimestampColumn(any(Map.class)))
                .thenReturn(null);
            
            // 应该抛出ValidationException（缺少必需字段），但不是JSON格式错误
            ValidationException exception = assertThrows(ValidationException.class, () -> 
                handler.handlePost(request, connection, "test_table", context)
            );
            
            // 应该是MissingRequiredField，不是InvalidFormat
            assertEquals("MissingRequiredField", exception.getErrorCode().getCode());
        }
    }
}
