package com.yugabyte.v2.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.yugabyte.v2.exception.ErrorCode;
import com.yugabyte.v2.exception.ValidationException;
import com.yugabyte.v2.service.DatabaseMetadataServiceV2.ColumnMetadata;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * 数据类型验证器
 * 用于验证JSON数据是否符合数据库列的数据类型要求
 */
public class DataTypeValidator {
    
    // 数值类型集合
    private static final Set<String> NUMERIC_TYPES = new HashSet<>();
    // 整数类型集合
    private static final Set<String> INTEGER_TYPES = new HashSet<>();
    // 浮点数类型集合
    private static final Set<String> FLOAT_TYPES = new HashSet<>();
    // 字符串类型集合
    private static final Set<String> STRING_TYPES = new HashSet<>();
    // 布尔类型集合
    private static final Set<String> BOOLEAN_TYPES = new HashSet<>();
    // 日期时间类型集合
    private static final Set<String> DATETIME_TYPES = new HashSet<>();
    // UUID类型集合
    private static final Set<String> UUID_TYPES = new HashSet<>();
    // JSON类型集合
    private static final Set<String> JSON_TYPES = new HashSet<>();
    
    static {
        // 初始化类型集合
        initializeTypeSets();
    }
    
    private static void initializeTypeSets() {
        // 整数类型
        INTEGER_TYPES.add("int");
        INTEGER_TYPES.add("integer");
        INTEGER_TYPES.add("smallint");
        INTEGER_TYPES.add("bigint");
        INTEGER_TYPES.add("serial");
        INTEGER_TYPES.add("bigserial");
        
        // 浮点数类型
        FLOAT_TYPES.add("float");
        FLOAT_TYPES.add("double");
        FLOAT_TYPES.add("real");
        FLOAT_TYPES.add("numeric");
        FLOAT_TYPES.add("decimal");
        
        // 数值类型 = 整数类型 + 浮点数类型
        NUMERIC_TYPES.addAll(INTEGER_TYPES);
        NUMERIC_TYPES.addAll(FLOAT_TYPES);
        
        // 字符串类型
        STRING_TYPES.add("varchar");
        STRING_TYPES.add("char");
        STRING_TYPES.add("text");
        STRING_TYPES.add("character");
        STRING_TYPES.add("character varying");
        
        // 布尔类型
        BOOLEAN_TYPES.add("boolean");
        BOOLEAN_TYPES.add("bool");
        
        // 日期时间类型
        DATETIME_TYPES.add("timestamp");
        DATETIME_TYPES.add("timestamptz");
        DATETIME_TYPES.add("date");
        DATETIME_TYPES.add("time");
        DATETIME_TYPES.add("datetime");
        
        // UUID类型
        UUID_TYPES.add("uuid");
        
        // JSON类型
        JSON_TYPES.add("json");
        JSON_TYPES.add("jsonb");
    }
    
    /**
     * 验证JSON值是否符合列的数据类型
     * 
     * @param columnName 列名
     * @param columnMetadata 列元数据
     * @param jsonValue JSON值
     * @param tableName 表名
     * @throws ValidationException 如果数据类型不匹配
     */
    public static void validateDataType(String columnName, ColumnMetadata columnMetadata, 
                                       JsonElement jsonValue, String tableName) {
        if (jsonValue == null || jsonValue.isJsonNull()) {
            // null值验证由其他逻辑处理
            return;
        }
        
        String typeName = columnMetadata.typeName().toLowerCase();
        
        try {
            if (NUMERIC_TYPES.contains(typeName)) {
                validateNumericType(columnName, typeName, jsonValue, tableName);
            } else if (STRING_TYPES.contains(typeName)) {
                validateStringType(columnName, typeName, jsonValue, tableName);
            } else if (BOOLEAN_TYPES.contains(typeName)) {
                validateBooleanType(columnName, typeName, jsonValue, tableName);
            } else if (DATETIME_TYPES.contains(typeName)) {
                validateDateTimeType(columnName, typeName, jsonValue, tableName);
            } else if (UUID_TYPES.contains(typeName)) {
                validateUuidType(columnName, typeName, jsonValue, tableName);
            } else if (JSON_TYPES.contains(typeName)) {
                validateJsonType(columnName, typeName, jsonValue, tableName);
            } else {
                // 未知类型，跳过验证
                // 在实际应用中，可以记录日志或抛出异常
            }
        } catch (ValidationException e) {
            throw e;
        } catch (Exception e) {
            throw ValidationException.invalidDataType(columnName, typeName, tableName);
        }
    }
    
    /**
     * 验证数值类型
     */
    private static void validateNumericType(String columnName, String typeName, 
                                           JsonElement jsonValue, String tableName) {
        if (!jsonValue.isJsonPrimitive()) {
            throw ValidationException.invalidDataType(columnName, typeName, tableName);
        }
        
        JsonPrimitive primitive = jsonValue.getAsJsonPrimitive();
        
        if (INTEGER_TYPES.contains(typeName)) {
            // 整数类型验证
            if (primitive.isNumber()) {
                try {
                    // 检查是否为整数
                    double value = primitive.getAsDouble();
                    if (value != Math.floor(value)) {
                        throw ValidationException.invalidDataType(columnName, typeName, tableName);
                    }
                } catch (NumberFormatException e) {
                    throw ValidationException.invalidDataType(columnName, typeName, tableName);
                }
            } else if (primitive.isString()) {
                try {
                    // 尝试解析字符串为整数
                    String strValue = primitive.getAsString();
                    Long.parseLong(strValue);
                } catch (NumberFormatException e) {
                    throw ValidationException.invalidDataType(columnName, typeName, tableName);
                }
            } else {
                throw ValidationException.invalidDataType(columnName, typeName, tableName);
            }
        } else if (FLOAT_TYPES.contains(typeName)) {
            // 浮点数类型验证
            if (primitive.isNumber()) {
                // 数字类型直接通过
                return;
            } else if (primitive.isString()) {
                try {
                    // 尝试解析字符串为浮点数
                    String strValue = primitive.getAsString();
                    new BigDecimal(strValue);
                } catch (NumberFormatException e) {
                    throw ValidationException.invalidDataType(columnName, typeName, tableName);
                }
            } else {
                throw ValidationException.invalidDataType(columnName, typeName, tableName);
            }
        }
    }
    
    /**
     * 验证字符串类型
     */
    private static void validateStringType(String columnName, String typeName, 
                                          JsonElement jsonValue, String tableName) {
        if (!jsonValue.isJsonPrimitive()) {
            throw ValidationException.invalidDataType(columnName, typeName, tableName);
        }
        
        JsonPrimitive primitive = jsonValue.getAsJsonPrimitive();
        
        if (!primitive.isString() && !primitive.isNumber() && !primitive.isBoolean()) {
            // 字符串类型可以接受字符串、数字和布尔值（会自动转换）
            throw ValidationException.invalidDataType(columnName, typeName, tableName);
        }
    }
    
    /**
     * 验证布尔类型
     */
    private static void validateBooleanType(String columnName, String typeName, 
                                           JsonElement jsonValue, String tableName) {
        if (!jsonValue.isJsonPrimitive()) {
            throw ValidationException.invalidDataType(columnName, typeName, tableName);
        }
        
        JsonPrimitive primitive = jsonValue.getAsJsonPrimitive();
        
        if (!primitive.isBoolean()) {
            // 如果不是布尔值，尝试检查是否为可转换的字符串
            if (primitive.isString()) {
                String strValue = primitive.getAsString().toLowerCase();
                if (!strValue.equals("true") && !strValue.equals("false") && 
                    !strValue.equals("1") && !strValue.equals("0")) {
                    throw ValidationException.invalidDataType(columnName, typeName, tableName);
                }
            } else if (primitive.isNumber()) {
                // 数字0或1可以转换为布尔值
                int numValue = primitive.getAsInt();
                if (numValue != 0 && numValue != 1) {
                    throw ValidationException.invalidDataType(columnName, typeName, tableName);
                }
            } else {
                throw ValidationException.invalidDataType(columnName, typeName, tableName);
            }
        }
    }
    
    /**
     * 验证日期时间类型
     */
    private static void validateDateTimeType(String columnName, String typeName, 
                                            JsonElement jsonValue, String tableName) {
        if (!jsonValue.isJsonPrimitive()) {
            throw ValidationException.invalidDataType(columnName, typeName, tableName);
        }
        
        JsonPrimitive primitive = jsonValue.getAsJsonPrimitive();
        
        if (primitive.isString()) {
            String strValue = primitive.getAsString();
            
            try {
                // 尝试解析常见的日期时间格式
                if (typeName.contains("date")) {
                    LocalDate.parse(strValue, DateTimeFormatter.ISO_LOCAL_DATE);
                } else if (typeName.contains("time")) {
                    // 尝试ISO格式
                    LocalDateTime.parse(strValue, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                }
            } catch (DateTimeParseException e) {
                throw ValidationException.invalidDataType(columnName, typeName, tableName);
            }
        } else if (!primitive.isNumber()) {
            // 日期时间类型通常接受字符串或数字（时间戳）
            throw ValidationException.invalidDataType(columnName, typeName, tableName);
        }
    }
    
    /**
     * 验证UUID类型
     */
    private static void validateUuidType(String columnName, String typeName, 
                                        JsonElement jsonValue, String tableName) {
        if (!jsonValue.isJsonPrimitive()) {
            throw ValidationException.invalidDataType(columnName, typeName, tableName);
        }
        
        JsonPrimitive primitive = jsonValue.getAsJsonPrimitive();
        
        if (primitive.isString()) {
            String strValue = primitive.getAsString();
            try {
                UUID.fromString(strValue);
            } catch (IllegalArgumentException e) {
                throw ValidationException.invalidDataType(columnName, typeName, tableName);
            }
        } else {
            throw ValidationException.invalidDataType(columnName, typeName, tableName);
        }
    }
    
    /**
     * 验证JSON类型
     */
    private static void validateJsonType(String columnName, String typeName, 
                                        JsonElement jsonValue, String tableName) {
        // JSON类型可以接受任何有效的JSON值
        // 不需要额外验证，因为Gson已经确保它是有效的JSON
        if (jsonValue.isJsonNull() || jsonValue.isJsonObject() || 
            jsonValue.isJsonArray() || jsonValue.isJsonPrimitive()) {
            return;
        }
        
        throw ValidationException.invalidDataType(columnName, typeName, tableName);
    }
    
    /**
     * 检查是否为数值类型
     */
    public static boolean isNumericType(String typeName) {
        return NUMERIC_TYPES.contains(typeName.toLowerCase());
    }
    
    /**
     * 检查是否为字符串类型
     */
    public static boolean isStringType(String typeName) {
        return STRING_TYPES.contains(typeName.toLowerCase());
    }
    
    /**
     * 检查是否为布尔类型
     */
    public static boolean isBooleanType(String typeName) {
        return BOOLEAN_TYPES.contains(typeName.toLowerCase());
    }
    
    /**
     * 检查是否为日期时间类型
     */
    public static boolean isDateTimeType(String typeName) {
        return DATETIME_TYPES.contains(typeName.toLowerCase());
    }
    
    /**
     * 检查是否为UUID类型
     */
    public static boolean isUuidType(String typeName) {
        return UUID_TYPES.contains(typeName.toLowerCase());
    }
    
    /**
     * 检查是否为JSON类型
     */
    public static boolean isJsonType(String typeName) {
        return JSON_TYPES.contains(typeName.toLowerCase());
    }
}
