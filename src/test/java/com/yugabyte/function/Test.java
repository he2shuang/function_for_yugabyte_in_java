package com.yugabyte.function;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class Test {
    public static void main(String[] args) {
        String str = "staring";
        try {
            JsonElement element = JsonParser.parseString(str);
            System.out.println(element); // 输出什么？
            
            System.out.println(element.getClass()); // class com.google.gson.JsonPrimitive
            System.out.println(element.isJsonPrimitive()); // true
            System.out.println(element.getAsJsonPrimitive().isString()); // false
            System.out.println(element.getAsJsonPrimitive().isNumber()); // false
            System.out.println(element.getAsJsonPrimitive().isBoolean()); // false
            // 那么它是什么类型？实际上，它是一个字符串，但是isString()返回false？这不可能。
            System.out.println(element.getAsString()); // "staring"
        } catch (Exception e) {
            System.out.println("抛出异常");
        }
    }
}