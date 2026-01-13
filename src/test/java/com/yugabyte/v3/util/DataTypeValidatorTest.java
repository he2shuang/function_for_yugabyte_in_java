package com.yugabyte.v3.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.yugabyte.v3.exception.ValidationException;
import com.yugabyte.v3.service.DatabaseMetadataServiceV3.ColumnMetadata;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DataTypeValidator 测试类
 */
class DataTypeValidatorTest {
    
    @Test
    void testValidateJsonTypeWithValidJsonObject() {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("name", "test");
        jsonObject.addProperty("value", 123);
        
        ColumnMetadata metadata = new ColumnMetadata("jsonb", true);
        
        // 应该不抛出异常
        assertDoesNotThrow(() -> 
            DataTypeValidator.validateDataType("test_column", metadata, jsonObject, "test_table")
        );
    }
    
    @Test
    void testValidateJsonTypeWithValidJsonArray() {
        JsonElement jsonArray = JsonParser.parseString("[1, 2, 3]");
        
        ColumnMetadata metadata = new ColumnMetadata("jsonb", true);
        
        // 应该不抛出异常
        assertDoesNotThrow(() -> 
            DataTypeValidator.validateDataType("test_column", metadata, jsonArray, "test_table")
        );
    }
    
    @Test
    void testValidateJsonTypeWithValidJsonString() {
        JsonPrimitive jsonString = new JsonPrimitive("{\"name\": \"test\"}");
        
        ColumnMetadata metadata = new ColumnMetadata("jsonb", true);
        
        // 应该不抛出异常（有效的JSON字符串）
        assertDoesNotThrow(() -> 
            DataTypeValidator.validateDataType("test_column", metadata, jsonString, "test_table")
        );
    }
    
    @Test
    void testValidateJsonTypeWithInvalidJsonString() {
        JsonPrimitive invalidJsonString = new JsonPrimitive("not a json string");
        
        ColumnMetadata metadata = new ColumnMetadata("jsonb", true);
        
        // 应该抛出ValidationException
        ValidationException exception = assertThrows(ValidationException.class, () -> 
            DataTypeValidator.validateDataType("test_column", metadata, invalidJsonString, "test_table")
        );
        
        assertEquals("InvalidFormat", exception.getErrorCode().getCode());
        assertEquals("test_column", exception.getErrorName());
        assertTrue(exception.getErrorDetail().contains("jsonb"));
    }
    
    @Test
    void testValidateJsonTypeWithNumber() {
        JsonPrimitive number = new JsonPrimitive(123);
        
        ColumnMetadata metadata = new ColumnMetadata("jsonb", true);
        
        // 应该抛出ValidationException（数字不是有效的JSON类型）
        ValidationException exception = assertThrows(ValidationException.class, () -> 
            DataTypeValidator.validateDataType("test_column", metadata, number, "test_table")
        );
        
        assertEquals("InvalidFormat", exception.getErrorCode().getCode());
    }
    
    @Test
    void testValidateJsonTypeWithBoolean() {
        JsonPrimitive bool = new JsonPrimitive(true);
        
        ColumnMetadata metadata = new ColumnMetadata("jsonb", true);
        
        // 应该抛出ValidationException（布尔值不是有效的JSON类型）
        ValidationException exception = assertThrows(ValidationException.class, () -> 
            DataTypeValidator.validateDataType("test_column", metadata, bool, "test_table")
        );
        
        assertEquals("InvalidFormat", exception.getErrorCode().getCode());
    }
    
    @Test
    void testValidateJsonTypeWithNull() {
        JsonElement nullElement = JsonParser.parseString("null");
        
        ColumnMetadata metadata = new ColumnMetadata("jsonb", true);
        
        // null值应该通过验证（由其他逻辑处理）
        assertDoesNotThrow(() -> 
            DataTypeValidator.validateDataType("test_column", metadata, nullElement, "test_table")
        );
    }
    
    @Test
    void testValidateJsonTypeWithEmptyObject() {
        JsonObject emptyObject = new JsonObject();
        
        ColumnMetadata metadata = new ColumnMetadata("jsonb", true);
        
        // 空对象应该通过验证
        assertDoesNotThrow(() -> 
            DataTypeValidator.validateDataType("test_column", metadata, emptyObject, "test_table")
        );
    }
    
    @Test
    void testValidateJsonTypeWithNestedJson() {
        JsonObject nestedJson = new JsonObject();
        nestedJson.addProperty("name", "test");
        JsonObject inner = new JsonObject();
        inner.addProperty("value", 123);
        nestedJson.add("inner", inner);
        
        ColumnMetadata metadata = new ColumnMetadata("jsonb", true);
        
        // 嵌套JSON应该通过验证
        assertDoesNotThrow(() -> 
            DataTypeValidator.validateDataType("test_column", metadata, nestedJson, "test_table")
        );
    }
    
    @Test
    void testValidateJsonTypeWithComplexJsonString() {
        JsonPrimitive complexJsonString = new JsonPrimitive("{\"name\": \"test\", \"values\": [1, 2, 3], \"nested\": {\"key\": \"value\"}}");
        
        ColumnMetadata metadata = new ColumnMetadata("jsonb", true);
        
        // 复杂的JSON字符串应该通过验证
        assertDoesNotThrow(() -> 
            DataTypeValidator.validateDataType("test_column", metadata, complexJsonString, "test_table")
        );
    }
}
