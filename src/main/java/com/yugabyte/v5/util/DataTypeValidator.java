package com.yugabyte.v5.util;

import com.yugabyte.v5.exception.ValidationException;

/**
 * データ型バリデーター
 * データ型の検証を担当
 */
public class DataTypeValidator {
    
    /**
     * データ型を検証
     */
    public static void validateDataType(Object value, String expectedType, String column) {
        if (value == null) {
            return; // NULL値は許可
        }
        
        String typeName = expectedType.toUpperCase();
        
        try {
            switch (typeName) {
                case "INTEGER":
                case "INT":
                case "BIGINT":
                case "SMALLINT":
                    validateInteger(value, column);
                    break;
                    
                case "DECIMAL":
                case "NUMERIC":
                case "FLOAT":
                case "DOUBLE":
                case "REAL":
                    validateDecimal(value, column);
                    break;
                    
                case "BOOLEAN":
                case "BOOL":
                    validateBoolean(value, column);
                    break;
                    
                case "DATE":
                    validateDate(value, column);
                    break;
                    
                case "TIMESTAMP":
                case "DATETIME":
                    validateTimestamp(value, column);
                    break;
                    
                case "VARCHAR":
                case "TEXT":
                case "CHAR":
                case "STRING":
                    validateString(value, column);
                    break;
                    
                default:
                    // 未知の型は文字列として扱う
                    validateString(value, column);
            }
        } catch (ValidationException e) {
            throw e;
        } catch (Exception e) {
            throw ValidationException.invalidFormat(column, 
                String.format("型 '%s' に変換できません: %s", expectedType, e.getMessage()));
        }
    }
    
    /**
     * クエリパラメータのデータ型を検証
     */
    public static void validateQueryParameter(String value, String expectedType, String column) {
        if (value == null || value.trim().isEmpty()) {
            throw ValidationException.notNull(column);
        }
        
        String typeName = expectedType.toUpperCase();
        
        try {
            switch (typeName) {
                case "INTEGER":
                case "INT":
                case "BIGINT":
                case "SMALLINT":
                    Long.parseLong(value);
                    break;
                    
                case "DECIMAL":
                case "NUMERIC":
                case "FLOAT":
                case "DOUBLE":
                case "REAL":
                    Double.parseDouble(value);
                    break;
                    
                case "BOOLEAN":
                case "BOOL":
                    validateBooleanString(value);
                    break;
                    
                case "DATE":
                    // 日付形式の簡易検証
                    if (!value.matches("\\d{4}-\\d{2}-\\d{2}")) {
                        throw ValidationException.invalidFormat(column, 
                            "日付形式は 'YYYY-MM-DD' である必要があります");
                    }
                    break;
                    
                case "TIMESTAMP":
                case "DATETIME":
                    // タイムスタンプ形式の簡易検証
                    if (!value.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")) {
                        throw ValidationException.invalidFormat(column, 
                            "タイムスタンプ形式は 'YYYY-MM-DD HH:MM:SS' である必要があります");
                    }
                    break;
                    
                default:
                    // 文字列型は常に有効
                    break;
            }
        } catch (NumberFormatException e) {
            throw ValidationException.invalidFormat(column, 
                String.format("数値に変換できません: %s", value));
        } catch (ValidationException e) {
            throw e;
        } catch (Exception e) {
            throw ValidationException.invalidFormat(column, 
                String.format("型 '%s' に変換できません: %s", expectedType, e.getMessage()));
        }
    }
    
    /**
     * 整数値の検証
     */
    private static void validateInteger(Object value, String column) {
        if (value instanceof Integer || value instanceof Long || 
            value instanceof Short || value instanceof Byte) {
            return;
        } else if (value instanceof String) {
            try {
                Long.parseLong((String) value);
            } catch (NumberFormatException e) {
                throw ValidationException.invalidFormat(column, "整数値である必要があります");
            }
        } else if (value instanceof Number) {
            // Number型は許可
            return;
        } else {
            throw ValidationException.invalidFormat(column, "整数値である必要があります");
        }
    }
    
    /**
     * 小数値の検証
     */
    private static void validateDecimal(Object value, String column) {
        if (value instanceof Double || value instanceof Float) {
            return;
        } else if (value instanceof String) {
            try {
                Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                throw ValidationException.invalidFormat(column, "小数値である必要があります");
            }
        } else if (value instanceof Number) {
            // Number型は許可
            return;
        } else {
            throw ValidationException.invalidFormat(column, "小数値である必要があります");
        }
    }
    
    /**
     * 真偽値の検証
     */
    private static void validateBoolean(Object value, String column) {
        if (value instanceof Boolean) {
            return;
        } else if (value instanceof String) {
            validateBooleanString((String) value);
        } else {
            throw ValidationException.invalidFormat(column, "真偽値である必要があります");
        }
    }
    
    /**
     * 文字列の真偽値検証
     */
    private static void validateBooleanString(String value) {
        String lowerValue = value.toLowerCase();
        if (!lowerValue.equals("true") && !lowerValue.equals("false") && 
            !lowerValue.equals("1") && !lowerValue.equals("0")) {
            throw ValidationException.invalidFormat("boolean", 
                "真偽値は 'true', 'false', '1', '0' のいずれかである必要があります");
        }
    }
    
    /**
     * 日付値の検証
     */
    private static void validateDate(Object value, String column) {
        if (value instanceof java.sql.Date || value instanceof java.util.Date) {
            return;
        } else if (value instanceof String) {
            // 簡易検証
            String strValue = (String) value;
            if (!strValue.matches("\\d{4}-\\d{2}-\\d{2}")) {
                throw ValidationException.invalidFormat(column, 
                    "日付形式は 'YYYY-MM-DD' である必要があります");
            }
        } else {
            throw ValidationException.invalidFormat(column, "日付値である必要があります");
        }
    }
    
    /**
     * タイムスタンプ値の検証
     */
    private static void validateTimestamp(Object value, String column) {
        if (value instanceof java.sql.Timestamp || value instanceof java.util.Date) {
            return;
        } else if (value instanceof String) {
            // 簡易検証
            String strValue = (String) value;
            if (!strValue.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")) {
                throw ValidationException.invalidFormat(column, 
                    "タイムスタンプ形式は 'YYYY-MM-DD HH:MM:SS' である必要があります");
            }
        } else {
            throw ValidationException.invalidFormat(column, "タイムスタンプ値である必要があります");
        }
    }
    
    /**
     * 文字列値の検証
     */
    private static void validateString(Object value, String column) {
        if (value instanceof String) {
            return;
        } else {
            // 文字列以外も許可（自動変換）
            return;
        }
    }
}
