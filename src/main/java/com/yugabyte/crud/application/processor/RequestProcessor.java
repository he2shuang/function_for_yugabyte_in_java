package com.yugabyte.crud.application.processor;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.yugabyte.crud.domain.exception.ValidationException;
import com.yugabyte.crud.domain.exception.ErrorCode;

import java.util.HashMap;
import java.util.Map;

/**
 * リクエストプロセッサ
 * リクエストボディの解析と変換を担当
 */
public class RequestProcessor {
    
    private static final Gson gson = new GsonBuilder()
        .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        .create();
    
    /**
     * リクエストボディを解析してMapに変換
     */
    public Map<String, Object> parseRequestBody(String jsonBody, String table) {
        if (jsonBody == null || jsonBody.isEmpty()) {
            throw ValidationException.missingBody();
        }
        
        try {
            JsonObject jsonObject = gson.fromJson(jsonBody, JsonObject.class);
            return convertJsonObjectToMap(jsonObject);
        } catch (JsonSyntaxException e) {
            throw ValidationException.invalidFormat("request_body", "無効なJSON形式: " + e.getMessage());
        }
    }
    
    /**
     * JsonObjectをMap<String, Object>に変換
     */
    private Map<String, Object> convertJsonObjectToMap(JsonObject jsonObject) {
        Map<String, Object> result = new HashMap<>();
        
        for (String key : jsonObject.keySet()) {
            Object value = convertJsonElement(jsonObject.get(key));
            result.put(key, value);
        }
        
        return result;
    }
    
    /**
     * JsonElementをJavaオブジェクトに変換
     */
    private Object convertJsonElement(com.google.gson.JsonElement element) {
        if (element == null || element.isJsonNull()) {
            return null;
        } else if (element.isJsonObject()) {
            return convertJsonObjectToMap(element.getAsJsonObject());
        } else if (element.isJsonArray()) {
            return gson.fromJson(element, Object[].class);
        } else if (element.isJsonPrimitive()) {
            com.google.gson.JsonPrimitive primitive = element.getAsJsonPrimitive();
            if (primitive.isBoolean()) {
                return primitive.getAsBoolean();
            } else if (primitive.isNumber()) {
                Number number = primitive.getAsNumber();
                // 整数か小数かを判断
                if (number.doubleValue() == number.longValue()) {
                    return number.longValue();
                } else {
                    return number.doubleValue();
                }
            } else {
                return primitive.getAsString();
            }
        }
        return null;
    }
    
    /**
     * クエリパラメータを検証
     */
    public void validateQueryParameters(Map<String, String> queryParams, String table) {
        if (queryParams == null || queryParams.isEmpty()) {
            throw ValidationException.missingFilter();
        }
    }
    
    /**
     * リクエストデータを検証
     */
    public void validateRequestData(Map<String, Object> data, String table) {
        if (data == null || data.isEmpty()) {
            throw ValidationException.noValidColumns();
        }
    }
}
