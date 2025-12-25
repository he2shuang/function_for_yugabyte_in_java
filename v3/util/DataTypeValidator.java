package com.yugabyte.v3.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.yugabyte.v3.exception.ErrorCode;
import com.yugabyte.v3.exception.ValidationException;
import com.yugabyte.v3.service.DatabaseMetadataServiceV3.ColumnMetadata;

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
    // 文字列型セット
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
        
        // 文字列型
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
     * JSON値が列のデータ型に準拠しているか検証
     * 
     * @param columnName 列名
     * @param columnMetadata 列メタデータ
     * @param jsonValue JSON値
     * @param tableName テーブル名
     * @throws ValidationException データ型が一致しない場合
     */
    public static void validateDataType(String columnName, ColumnMetadata columnMetadata, 
                                       JsonElement jsonValue, String tableName) {
        if (jsonValue == null || jsonValue.isJsonNull()) {
            // null値の検証は他のロジックで処理
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
                // 未知の型、検証をスキップ
                // 実際のアプリケーションでは、ログを記録するか例外をスローできます
            }
        } catch (ValidationException e) {
            throw e;
        } catch (Exception e) {
            throw ValidationException.invalidDataType(columnName, typeName, tableName);
        }
    }
    
    /**
     * 数値型を検証
     */
    private static void validateNumericType(String columnName, String typeName, 
                                           JsonElement jsonValue, String tableName) {
        if (!jsonValue.isJsonPrimitive()) {
            throw ValidationException.invalidDataType(columnName, typeName, tableName);
        }
        
        JsonPrimitive primitive = jsonValue.getAsJsonPrimitive();
        
        if (INTEGER_TYPES.contains(typeName)) {
            // 整数型検証
            if (primitive.isNumber()) {
                try {
                    // 整数かどうかをチェック
                    double value = primitive.getAsDouble();
                    if (value != Math.floor(value)) {
                        throw ValidationException.invalidDataType(columnName, typeName, tableName);
                    }
                } catch (NumberFormatException e) {
                    throw ValidationException.invalidDataType(columnName, typeName, tableName);
                }
            } else if (primitive.isString()) {
                try {
                    // 文字列を整数として解析を試みる
                    String strValue = primitive.getAsString();
                    Long.parseLong(strValue);
                } catch (NumberFormatException e) {
                    throw ValidationException.invalidDataType(columnName, typeName, tableName);
                }
            } else {
                throw ValidationException.invalidDataType(columnName, typeName, tableName);
            }
        } else if (FLOAT_TYPES.contains(typeName)) {
            // 浮動小数点型検証
            if (primitive.isNumber()) {
                // 数値型は直接通過
                return;
            } else if (primitive.isString()) {
                try {
                    // 文字列を浮動小数点数として解析を試みる
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
     * 文字列型を検証
     */
    private static void validateStringType(String columnName, String typeName, 
                                          JsonElement jsonValue, String tableName) {
        if (!jsonValue.isJsonPrimitive()) {
            throw ValidationException.invalidDataType(columnName, typeName, tableName);
        }
        
        JsonPrimitive primitive = jsonValue.getAsJsonPrimitive();
        
        if (!primitive.isString() && !primitive.isNumber() && !primitive.isBoolean()) {
            // 文字列型は文字列、数値、ブール値を受け入れる（自動変換）
            throw ValidationException.invalidDataType(columnName, typeName, tableName);
        }
    }
    
    /**
     * ブール型を検証
     */
    private static void validateBooleanType(String columnName, String typeName, 
                                           JsonElement jsonValue, String tableName) {
        if (!jsonValue.isJsonPrimitive()) {
            throw ValidationException.invalidDataType(columnName, typeName, tableName);
        }
        
        JsonPrimitive primitive = jsonValue.getAsJsonPrimitive();
        
        if (!primitive.isBoolean()) {
            // ブール値でない場合、変換可能な文字列かチェック
            if (primitive.isString()) {
                String strValue = primitive.getAsString().toLowerCase();
                if (!strValue.equals("true") && !strValue.equals("false") && 
                    !strValue.equals("1") && !strValue.equals("0")) {
                    throw ValidationException.invalidDataType(columnName, typeName, tableName);
                }
            } else if (primitive.isNumber()) {
                // 数値0または1はブール値に変換可能
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
     * 日時型を検証
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
                // 一般的な日時形式の解析を試みる
                if (typeName.contains("date")) {
                    LocalDate.parse(strValue, DateTimeFormatter.ISO_LOCAL_DATE);
                } else if (typeName.contains("time")) {
                    // ISO形式を試みる
                    LocalDateTime.parse(strValue, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                }
            } catch (DateTimeParseException e) {
                throw ValidationException.invalidDataType(columnName, typeName, tableName);
            }
        } else if (!primitive.isNumber()) {
            // 日時型は通常、文字列または数値（タイムスタンプ）を受け入れる
            throw ValidationException.invalidDataType(columnName, typeName, tableName);
        }
    }
    
    /**
     * UUID型を検証
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
     * JSON型を検証
     */
    private static void validateJsonType(String columnName, String typeName, 
                                        JsonElement jsonValue, String tableName) {
        // JSON型は任意の有効なJSON値を受け入れる
        // Gsonがすでに有効なJSONであることを保証しているため、追加の検証は不要
        if (jsonValue.isJsonNull() || jsonValue.isJsonObject() || 
            jsonValue.isJsonArray() || jsonValue.isJsonPrimitive()) {
            return;
        }
        
        throw ValidationException.invalidDataType(columnName, typeName, tableName);
    }
    
    /**
     * 数値型かどうかをチェック
     */
    public static boolean isNumericType(String typeName) {
        return NUMERIC_TYPES.contains(typeName.toLowerCase());
    }
    
    /**
     * 文字列型かどうかをチェック
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
