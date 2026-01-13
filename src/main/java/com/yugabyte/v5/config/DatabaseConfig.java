package com.yugabyte.v5.config;

import com.yugabyte.v5.exception.DatabaseException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * データベース接続設定クラス
 * データベース接続の管理と設定を担当
 */
public class DatabaseConfig {
    
    private static final String DB_URL = System.getenv("DB_URL");
    private static final String DB_USER = System.getenv("DB_USER");
    private static final String DB_PASSWORD = System.getenv("DB_PASSWORD");
    
    /**
     * データベース接続を取得
     */
    public static Connection getConnection() {
        validateConfig();
        
        try {
            return DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        } catch (SQLException e) {
            throw DatabaseException.connectionFailed(e.getMessage(), e);
        }
    }
    
    /**
     * データベース接続設定を検証
     */
    private static void validateConfig() {
        if (DB_URL == null || DB_USER == null || DB_PASSWORD == null) {
            throw DatabaseException.configMissing("DB_URL、DB_USER、DB_PASSWORD環境変数が設定されていません");
        }
    }
    
    /**
     * 接続設定が有効かどうかを確認
     */
    public static boolean isConfigValid() {
        return DB_URL != null && DB_USER != null && DB_PASSWORD != null;
    }
    
    /**
     * テスト用の接続を取得（設定検証をスキップ）
     */
    public static Connection getConnectionForTest(String url, String user, String password) {
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            throw DatabaseException.connectionFailed(e.getMessage(), e);
        }
    }
}
