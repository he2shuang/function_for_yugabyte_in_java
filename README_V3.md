# YugabyteDB Azure Function V3 - 日本語版

## 概要

このプロジェクトは、YugabyteDBに接続し、CRUD操作を実行するAzure FunctionのV3バージョンです。V2バージョンから以下の改善が行われました：

1. **日本語化**: すべてのコメント、エラーメッセージ、ドキュメントを日本語に翻訳
2. **標準化されたエラー応答**: エラー応答にerrorCode、errorName、errorDetailを含む統一フォーマット
3. **強化されたエラー処理**: より詳細なエラーメッセージと適切なHTTPステータスコード

## エラー応答フォーマット

すべてのエラー応答は以下のJSONフォーマットに従います：

```json
{
  "success": false,
  "error": {
    "errorCode": "NotNull",
    "errorName": "apikey",
    "errorDetail": "フィールド 'apikey' は空にできません"
  },
  "timestamp": 1735099200000
}
```

## エラーコード一覧

### 検証エラー (400)
- `NotNull`: フィールドは空にできません
- `InvalidFormat`: 形式が無効です
- `MissingRequiredField`: 必須フィールドが不足しています
- `MissingBody`: リクエストボディは空にできません
- `MissingFilter`: フィルタ条件が不足しています
- `NoValidColumns`: 有効な列が提供されていません
- `NoValidFilters`: 有効なフィルタ条件が提供されていません
- `MethodNotSupported`: サポートされていないHTTPメソッドです
- `UnknownColumn`: 不明な列です

### データベースエラー (404/500)
- `TableNotFound`: テーブルが存在しません
- `ConnectionFailed`: データベース接続に失敗しました
- `QueryFailed`: SQLクエリの実行に失敗しました
- `NoPrimaryKey`: テーブルに主キーが定義されていません
- `DatabaseError`: データベース操作に失敗しました

### 設定エラー (500)
- `DbConfigMissing`: データベース接続設定が不足しています

### システムエラー (500)
- `InternalError`: システム内部エラー

## 使用方法

### エンドポイント
```
GET/POST/PATCH/DELETE /api/v3/{table}
```

### 環境変数
以下の環境変数を設定する必要があります：
- `DB_URL`: YugabyteDB接続URL
- `DB_USER`: データベースユーザー名
- `DB_PASSWORD`: データベースパスワード

### 例

#### GETリクエスト
```
GET /api/v3/users?id=123e4567-e89b-12d3-a456-426614174000
```

#### POSTリクエスト
```json
POST /api/v3/users
{
  "name": "山田太郎",
  "email": "taro.yamada@example.com",
  "age": 30
}
```

#### PATCHリクエスト
```json
PATCH /api/v3/users?id=123e4567-e89b-12d3-a456-426614174000
{
  "name": "山田花子",
  "age": 28
}
```

#### DELETEリクエスト
```
DELETE /api/v3/users?id=123e4567-e89b-12d3-a456-426614174000
```

## プロジェクト構造

```
src/main/java/com/yugabyte/v3/
├── dto/
│   └── ErrorResponse.java          # エラー応答DTO
├── exception/
│   ├── BusinessException.java      # ビジネス例外基底クラス
│   ├── DatabaseException.java      # データベース例外
│   ├── ErrorCode.java              # エラーコード列挙型
│   └── ValidationException.java    # 検証例外
├── function/
│   └── FunctionV3.java             # Azure Functionメインクラス
├── handler/
│   └── CrudHandlerV3.java          # CRUD操作ハンドラー
├── service/
│   └── DatabaseMetadataServiceV3.java # データベースメタデータサービス
└── util/
    ├── DataTypeValidator.java      # データ型バリデータ
    └── ResponseUtil.java           # レスポンスユーティリティ
```

## ビルドとデプロイ

### ビルド
```bash
mvn clean package
```

### ローカル実行
```bash
mvn azure-functions:run
```

### デプロイ
```bash
mvn azure-functions:deploy
```

## ライセンス

このプロジェクトはMITライセンスの下で提供されています。
