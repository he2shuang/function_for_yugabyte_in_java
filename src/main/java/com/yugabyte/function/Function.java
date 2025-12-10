package com.yugabyte.function;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.yugabyte.handler.CrudHandler;
import com.yugabyte.service.DatabaseMetadataService;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;

/**
 * Azure Function 的主入口类。
 */
public class Function {

    private static final CrudHandler crudHandler = new CrudHandler();

    /**
     * Azure Function 的主执行方法。
     *
     * @param request 包含请求所有信息（如方法、查询参数、请求体）的对象。
     * @param table   从 URL 路径中动态捕获的表名。
     * @param context Azure Function 的执行上下文，用于访问日志记录器等运行时功能。
     * @return 对客户端的 HTTP 响应。
     */
    @FunctionName("YugaCrudApi")
    public HttpResponseMessage run(
            @HttpTrigger(
                name = "req",
                methods = {HttpMethod.GET, HttpMethod.POST, HttpMethod.PATCH, HttpMethod.DELETE},
                authLevel = AuthorizationLevel.ANONYMOUS,
                route = "{table}")
            HttpRequestMessage<Optional<String>> request,
            @BindingName("table") String table,
            final ExecutionContext context) {

        context.getLogger().info("Java HTTP trigger processed a " + request.getHttpMethod() + " request for table: " + table);

        // 从环境变量 (或 local.settings.json) 中获取数据库连接信息
        String url = System.getenv("DB_URL");
        String user = System.getenv("DB_USER");
        String password = System.getenv("DB_PASSWORD");

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            // 动态安全检查：在分派请求前，验证目标表是否存在于数据库中。
            if (!DatabaseMetadataService.tableExists(conn, table, context)) {
                return request.createResponseBuilder(HttpStatus.NOT_FOUND)
                        .body("Table '" + table + "' not found.").build();
            }

            // 根据 HTTP 方法，将请求和数据库连接委托给 CrudHandler 进行处理。
            switch (request.getHttpMethod()) {
                case GET:
                    return crudHandler.handleGet(request, conn, table, context);
                case POST:
                    return crudHandler.handlePost(request, conn, table, context);
                case PATCH:
                    return crudHandler.handlePatch(request, conn, table, context);
                case DELETE:
                    return crudHandler.handleDelete(request, conn, table, context);
                default:
                    // 对于 @HttpTrigger 中未列出的方法，返回错误。
                    return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                            .body("HTTP method not supported.").build();
            }
        } catch (SQLException e) {
            // 捕获所有 JDBC 和数据库相关的异常
            context.getLogger().severe("SQL Exception: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Database error occurred: " + e.getMessage()).build();
        } catch (Exception e) {
            // 捕获所有其他意外异常，防止敏感信息泄露
            context.getLogger().severe("Generic Exception: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("An unexpected error occurred.").build();
        }
    }
}