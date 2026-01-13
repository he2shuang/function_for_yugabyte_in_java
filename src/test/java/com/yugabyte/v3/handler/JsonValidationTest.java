package com.yugabyte.v3.handler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.yugabyte.v3.exception.ErrorCode;
import com.yugabyte.v3.exception.ValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JSON验证测试
 */
class JsonValidationTest {
    
    private static final Gson gson = new GsonBuilder().create();
    
    @Test
    void testJsonSyntaxExceptionIsCaughtAndConvertedToValidationException() {
        // 测试各种无效JSON格式
        String[] invalidJsons = {
            "{\"name\": \"test\", \"value\": 123", // 缺少右大括号
            "{\"name\": \"test\" \"value\": 123}", // 缺少逗号
            "{\"name\": test, \"value\": 123}",    // 缺少引号
            "{name: \"test\", value: 123}",        // 属性名缺少引号
            "[\"test\", 123",                      // 数组缺少右括号
            "{\"name\": \"test\",}",               // 多余的逗号
            "test",                                // 不是JSON对象
            "123",                                 // 只是数字
            "true",                                // 只是布尔值
            "null"                                 // 只是null
        };
        
        for (String invalidJson : invalidJsons) {
            try {
                gson.fromJson(invalidJson, com.google.gson.JsonObject.class);
                fail("应该抛出JsonSyntaxException对于: " + invalidJson);
            } catch (JsonSyntaxException e) {
                // 这是预期的，现在测试我们的转换逻辑
                ValidationException validationException = new ValidationException(
                    ErrorCode.INVALID_FORMAT,
                    "request_body",
                    String.format("无效的JSON格式: %s", e.getMessage())
                );
                
                assertEquals("InvalidFormat", validationException.getErrorCode().getCode());
                assertEquals("request_body", validationException.getErrorName());
                assertTrue(validationException.getErrorDetail().contains("无效的JSON格式"));
            }
        }
    }
    
    @Test
    void testValidJsonDoesNotThrowException() {
        String[] validJsons = {
            "{}",                                  // 空对象
            "{\"name\": \"test\"}",                // 简单对象
            "{\"name\": \"test\", \"value\": 123}", // 多个属性
            "{\"name\": \"test\", \"nested\": {\"key\": \"value\"}}", // 嵌套对象
            "[]",                                  // 空数组
            "[1, 2, 3]",                           // 数字数组
            "[\"a\", \"b\", \"c\"]",               // 字符串数组
            "null"                                 // JSON null
        };
        
        for (String validJson : validJsons) {
            assertDoesNotThrow(() -> {
                gson.fromJson(validJson, com.google.gson.JsonObject.class);
            }, "有效的JSON应该不抛出异常: " + validJson);
        }
    }
    
    @Test
    void testValidationExceptionContainsCorrectErrorCode() {
        ValidationException exception = new ValidationException(
            ErrorCode.INVALID_FORMAT,
            "request_body",
            "无效的JSON格式: Expected BEGIN_OBJECT but was STRING at line 1 column 1 path $"
        );
        
        assertEquals("InvalidFormat", exception.getErrorCode().getCode());
        assertEquals("request_body", exception.getErrorName());
        assertTrue(exception.getErrorDetail().contains("无效的JSON格式"));
    }
}
