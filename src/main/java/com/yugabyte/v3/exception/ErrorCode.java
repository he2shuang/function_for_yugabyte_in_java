package com.yugabyte.v3.exception;

/**
 * エラーコード列挙型
 * すべてのエラーコードを集中管理
 */
public enum ErrorCode {
    
    // 検証エラー (400)
    NOT_NULL("NotNull", "フィールドは空にできません"),
    INVALID_FORMAT("InvalidFormat", "形式が無効です"),
    MISSING_REQUIRED_FIELD("MissingRequiredField", "必須フィールドが不足しています"),
    MISSING_BODY("MissingBody", "リクエストボディは空にできません"),
    MISSING_FILTER("MissingFilter", "フィルタ条件が不足しています"),
    NO_VALID_COLUMNS("NoValidColumns", "有効な列が提供されていません"),
    NO_VALID_FILTERS("NoValidFilters", "有効なフィルタ条件が提供されていません"),
    METHOD_NOT_SUPPORTED("MethodNotSupported", "サポートされていないHTTPメソッドです"),
    UNKNOWN_COLUMN("UnknownColumn", "不明な列です"),
    
    // データベースエラー (404/500)
    TABLE_NOT_FOUND("TableNotFound", "テーブルが存在しません"),
    CONNECTION_FAILED("ConnectionFailed", "データベース接続に失敗しました"),
    QUERY_FAILED("QueryFailed", "SQLクエリの実行に失敗しました"),
    NO_PRIMARY_KEY("NoPrimaryKey", "テーブルに主キーが定義されていません"),
    DATABASE_ERROR("DatabaseError", "データベース操作に失敗しました"),
    
    // 設定エラー (500)
    DB_CONFIG_MISSING("DbConfigMissing", "データベース接続設定が不足しています"),
    
    // システムエラー (500)
    INTERNAL_ERROR("InternalError", "システム内部エラー"),
    
    // HTTPメソッドエラー (405)
    METHOD_NOT_ALLOWED("MethodNotAllowed", "HTTPメソッドが許可されていません"),
    
    // タイムアウトエラー (504)
    GATEWAY_TIMEOUT("GatewayTimeout", "ゲートウェイタイムアウト"),
    
    // サービス利用不可 (503)
    SERVICE_UNAVAILABLE("ServiceUnavailable", "サービス利用不可");
    
    private final String code;
    private final String description;
    
    ErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }
    
    public String getCode() {
        return code;
    }
    
    public String getDescription() {
        return description;
    }
    
    @Override
    public String toString() {
        return code;
    }
}
