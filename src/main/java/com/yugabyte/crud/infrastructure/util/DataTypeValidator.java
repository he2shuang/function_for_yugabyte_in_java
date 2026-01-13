package com.yugabyte.crud.infrastructure.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.yugabyte.crud.domain.exception.ValidationException;
import com.yugabyte.crud.domain.repository.DatabaseMetadataRepository.ColumnMetadata;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * データ型バリデータ
 * JSONデータがデータベース列のデータ型要件に準拠しているか検証するために使用
 */
public class DataTypeValidator {
    
    // 数値型セット
    private static final Set<String> NUMERIC_TYPES = new HashSet<>();
    // 整数型セット
    private static final Set<String> INTEGER_TYPES = new HashSet<>();
    // 浮動小数点型セット
    private static final Set<String> FLOAT_TYPES = new HashSet<>();
    // 文字串型セット
    private static final Set<String> STRING_TYPES = new HashSet<>();
    // ブール型セット
    private static final Set<String> BOOLEAN_TYPES = new HashSet<>();
    // 日時型セット
    private static final Set<String> DATETIME_TYPES = new HashSet<>();
    // UUID型セット
    private static final Set<String> UUID_TYPES = new HashSet<>();
    // JSON型セット
    private static final Set<String> JSON_TYPES = new HashSet<>();
    
    static {
        // 型セットの初期化
        initializeTypeSets();
    }
    
    private static void initializeTypeSets() {
        // 整数型
        INTEGER_TYPES.add("int");
        INTEGER_TYPES.add("integer");
        INTEGER_TYPES.add("smallint");
        INTEGER_TYPES.add("bigint");
        INTEGER_TYPES.add("serial");
        INTEGER_TYPES.add("bigserial");
        
        // 浮動小数点型
        FLOAT_TYPES.add("float");
        FLOAT_TYPES.add("double");
        FLOAT_TYPES.add("real");
        FLOAT_TYPES.add("numeric");
        FLOAT_TYPES.add("decimal");
        
        // 数値型 = 整数型 + 浮動小数点型
        NUMERIC_TYPES.addAll(INTEGER_TYPES);
        NUMERIC_TYPES.addAll(FLOAT_TYPES);
        
        // 文字串型
        STRING_TYPES.add("varchar");
        STRING_TYPES.add("char");
        STRING_TYPES.add("text");
        STRING_TYPES.add("character");
        STRING_TYPES.add("character varying");
        
        // ブール型
        BOOLEAN_TYPES.add("boolean");
        BOOLEAN_TYPES.add("bool");
        
        // 日時型
        DATETIME_TYPES.add("timestamp");
        DATETIME_TYPES.add("timestamptz");
        DATETIME_TYPES.add("date");
        DATETIME_TYPES.add("time");
        DATETIME_TYPES.add("datetime");
        
        // UUID型
        UUID_TYPES.add("uuid");
        
        // JSON型
        JSON_TYPES.add("json");
        JSON_TYPES.add("jsonb");
    }
    
    /**
     * 値が列のデータ型に準拠しているか検証
     * 
     * @param columnName 列名
     * @param columnMetadata 列メタデータ
     * @param value 値
     * @param tableName テーブル名
     * @throws ValidationException データ型が一致しない場合
     */
    public static void validateDataType(String columnName, ColumnMetadata columnMetadata, 
                                       Object value, String tableName) {
        if (value == null) {
            // null値の検証は他のロジックで処理
            return;
        }
        
        String typeName = columnMetadata.typeName().toLowerCase();
        
        try {
            if (NUMERIC_TYPES.contains(typeName)) {
                validateNumericType(columnName, typeName, value, tableName);
            } else if (STRING_TYPES.contains(typeName)) {
                validateStringType(columnName, typeName, value, tableName);
            } else if (BOOLEAN_TYPES.contains(typeName)) {
                validateBooleanType(columnName, typeName, value, tableName);
            } else if (DATETIME_TYPES.contains(typeName)) {
                validateDateTimeType(columnName, typeName, value, tableName);
            } else if (UUID_TYPES.contains(typeName)) {
                validateUuidType(columnName, typeName, value, tableName);
            } else if (JSON_TYPES.contains(typeName)) {
                validateJsonType(columnName, typeName, value, tableName);
            } else {
                // 未知の型、検証をスキップ
                // 実際のアプリケーションでは、ログを記録するか例外をスローできます
            }
        } catch (ValidationException e) {
            throw e;
        } catch (Exception e) {
            throw ValidationException.invalidFormat(columnName, 
                String.format("データ型 '%s' に変換できません", typeName));
        }
    }
    
    /**
     * 数値型を検証
     */
    private static void validateNumericType(String columnName, String typeName, 
                                           Object value, String tableName) {
        if (value instanceof Number) {
            if (INTEGER_TYPES.contains(typeName)) {
                // 整数型検証
                double doubleValue = ((Number) value).doubleValue();
                if (doubleValue != Math.floor(doubleValue)) {
                    throw ValidationException.invalidFormat(columnName, 
                        String.format("整数型 '%s' に小数値は許可されません", typeName));
                }
            }
            // 浮動小数点型はすべての数値を受け入れる
        } else if (value instanceof String) {
            try {
                String strValue = (String) value;
                if (INTEGER_TYPES.contains(typeName)) {
                    Long.parseLong(strValue);
                } else {
                    new BigDecimal(strValue);
                }
            } catch (NumberFormatException e) {
                throw ValidationException.invalidFormat(columnName, 
                    String.format("数値型 '%s' に変換できません", typeName));
            }
        } else {
            throw ValidationException.invalidFormat(columnName, 
                String.format("数値型 '%s' に変換できません", typeName));
        }
    }
    
    /**
     * 文字串型を検証
     */
    private static void validateStringType(String columnName, String typeName, 
                                          Object value, String tableName) {
        // 文字串型はすべての型を受け入れる（自動変換）
        if (value instanceof String || value instanceof Number || 
            value instanceof Boolean || value == null) {
            return;
        }
        throw ValidationException.invalidFormat(columnName, 
            String.format("文字串型 '%s' に変換できません", typeName));
    }
    
    /**
     * ブール型を検証
     */
    private static void validateBooleanType(String columnName, String typeName, 
                                           Object value, String tableName) {
        if (value instanceof Boolean) {
            return;
        } else if (value instanceof String) {
            String strValue = ((String) value).toLowerCase();
            if (!strValue.equals("true") && !strValue.equals("false") && 
                !strValue.equals("1") && !strValue.equals("0")) {
                throw ValidationException.invalidFormat(columnName, 
                    String.format("ブール型 '%s' に変換できません", typeName));
            }
        } else if (value instanceof Number) {
            int numValue = ((Number) value).intValue();
            if (numValue != 0 && numValue != 1) {
                throw ValidationException.invalidFormat(columnName, 
                    String.format("ブール型 '%s' に変換できません", typeName));
            }
        } else {
            throw ValidationException.invalidFormat(columnName, 
                String.format("ブール型 '%s' に変換できません", typeName));
        }
    }
    
    /**
     * 日時型を検証
     */
    private static void validateDateTimeType(String columnName, String typeName, 
                                            Object value, String tableName) {
        if (value instanceof String) {
            String strValue = (String) value;
            
            try {
                // 一般的な日時形式の解析を試みる
                if (typeName.contains("date")) {
                    LocalDate.parse(strValue, DateTimeFormatter.ISO_LOCAL_DATE);
                } else if (typeName.contains("time")) {
                    // ISO形式を試みる
                    LocalDateTime.parse(strValue, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                }
            } catch (DateTimeParseException e) {
                throw ValidationException.invalidFormat(columnName, 
                    String.format("日時型 '%s' に変換できません", typeName));
            }
        } else if (!(value instanceof Number)) {
            // 日時型は通常、文字串または数値（タイムスタンプ）を受け入れる
            throw ValidationException.invalidFormat(columnName, 
                String.format("日時型 '%s' に変換できません", typeName));
        }
    }
    
    /**
     * UUID型を検証
     */
    private static void validateUuidType(String columnName, String typeName, 
                                        Object value, String tableName) {
        if (value instanceof String) {
            String strValue = (String) value;
            try {
                UUID.fromString(strValue);
            } catch (IllegalArgumentException e) {
                throw ValidationException.invalidFormat(columnName, 
                    String.format("UUID型 '%s' に変換できません", typeName));
            }
        } else {
            throw ValidationException.invalidFormat(columnName, 
                String.format("UUID型 '%s' に変換できません", typeName));
        }
    }
    
    /**
     * JSON型を検証
     */
    private static void validateJsonType(String columnName, String typeName, 
                                        Object value, String tableName) {
        String jsonString;
        
        if (value instanceof String) {
            jsonString = (String) value;
        } else {
            jsonString = value.toString();
        }

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            // 使用Jackson进行严格解析
            objectMapper.readTree(jsonString);
            // 解析成功，是有效的JSON
        } catch (JsonProcessingException e) {
            // 不是有效的JSON格式
            throw ValidationException.invalidFormat(columnName, 
                String.format("JSON型 '%s' に変換できません", typeName));
        }
    }
    
    /**
     * 数値型かどうかをチェック
     */
    public static boolean isNumericType(String typeName) {
        return NUMERIC_TYPES.contains(typeName.toLowerCase());
    }
    
    /**
     * 文字串型かどうかをチェック
     */
    public static boolean isStringType(String typeName) {
        return STRING_TYPES.contains(typeName.toLowerCase());
    }
    
    /**
     * ブール型かどうかをチェック
     */
    public static boolean isBooleanType(String typeName) {
        return BOOLEAN_TYPES.contains(typeName.toLowerCase());
    }
    
    /**
     * 日時型かどうかをチェック
     */
    public static boolean isDateTimeType(String typeName) {
        return DATETIME_TYPES.contains(typeName.toLowerCase());
    }
    
    /**
     * UUID型かどうかをチェック
     */
    public static boolean isUuidType(String typeName) {
        return UUID_TYPES.contains(typeName.toLowerCase());
    }
    
    /**
     * JSON型かどうかをチェック
     */
    public static boolean isJsonType(String typeName) {
        return JSON_TYPES.contains(typeName.toLowerCase());
    }
}
