package com.yugabyte.v2.function;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.yugabyte.v2.dto.ErrorResponse;
import com.yugabyte.v2.exception.BusinessException;
import com.yugabyte.v2.exception.DatabaseException;
import com.yugabyte.v2.exception.ErrorCode;
import com.yugabyte.v2.handler.CrudHandlerV2;
import com.yugabyte.v2.service.DatabaseMetadataServiceV2;
import com.yugabyte.v2.util.ResponseUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;

/**
 * Azure Function V2 - 主入口类
 * 增强错误处理和响应标准化
 */
public class FunctionV2 {
    
    private static final CrudHandlerV2 crudHandler = new CrudHandlerV2();
    
    /**
     * Azure Function V2 的主执行方法
     */
    @FunctionName("YugaCrudApiV2")
    public HttpResponseMessage run(
            @HttpTrigger(
                name = "req",
                methods = {HttpMethod.GET, HttpMethod.POST, HttpMethod.PATCH, HttpMethod.DELETE},
                authLevel = AuthorizationLevel.ANONYMOUS,
                route = "v2/{table}")
            HttpRequestMessage<Optional<String>> request,
            @BindingName("table") String table,
            final ExecutionContext context) {
        
        context.getLogger().info("Java HTTP trigger processed a " + request.getHttpMethod() + 
                                " request for table: " + table + " (V2)");
        
        // 从环境变量中获取数据库连接信息
        String url = System.getenv("DB_URL");
        String user = System.getenv("DB_USER");
        String password = System.getenv("DB_PASSWORD");
        
        // 验证环境变量
        if (url == null || user == null || password == null) {
            context.getLogger().severe("数据库连接环境变量未配置");
            return ResponseUtil.configError(request, 
                ErrorCode.DB_CONFIG_MISSING, 
                "environment_variables", 
                "数据库连接配置缺失，请检查DB_URL、DB_USER、DB_PASSWORD环境变量");
        }
        
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            // 动态安全检查：验证目标表是否存在于数据库中
            if (!DatabaseMetadataServiceV2.tableExists(conn, table, context)) {
                return ResponseUtil.error(request, HttpStatus.NOT_FOUND,
                    new ErrorResponse(
                        ErrorCode.TABLE_NOT_FOUND.getCode(),
                        table,
                        String.format("表 '%s' 不存在", table)
                    ));
            }
            
            // 根据HTTP方法，将请求和数据库连接委托给CrudHandlerV2进行处理
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
                    // 对于未列出的方法，返回错误
                    return ResponseUtil.validationError(request,
                        ErrorCode.METHOD_NOT_SUPPORTED,
                        "http_method",
                        "不支持的HTTP方法: " + request.getHttpMethod());
            }
            
        } catch (BusinessException e) {
            // 业务异常处理
            context.getLogger().warning("业务异常: " + e.getErrorDetail());
            return ResponseUtil.businessError(request, e);
            
        } catch (SQLException e) {
            // 数据库异常处理
            context.getLogger().severe("数据库异常: " + e.getMessage());
            return ResponseUtil.databaseError(request,
                ErrorCode.DATABASE_ERROR,
                "database_operation",
                "数据库操作失败: " + e.getMessage());
            
        } catch (Exception e) {
            // 其他未捕获异常处理
            context.getLogger().severe("未捕获异常: " + e.getMessage());
            return ResponseUtil.internalError(request, e);
        }
    }
}
