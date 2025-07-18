# CloudBerry MVP

这是一个使用Angular前端和Spring Boot后端的MVP项目。

## 项目结构

```
MVP/
├── frontend/          # Angular前端应用
└── backend/           # Spring Boot后端应用
```

## 运行项目

### 后端 (Spring Boot)

1. 进入后端目录：
   ```bash
   cd backend
   ```

2. 运行Spring Boot应用：
   ```bash
   ./mvnw spring-boot:run
   ```

后端将在 `http://localhost:8080` 运行。

### 前端 (Angular)

1. 进入前端目录：
   ```bash
   cd frontend
   ```

2. 安装依赖：
   ```bash
   npm install
   ```

3. 运行开发服务器：
   ```bash
   ng serve
   ```

前端将在 `http://localhost:3000` 运行。

## 功能特性

- 用户注册和登录
- JWT认证（使用HTTP-only cookie）
- 用户信息管理
- 响应式设计
- 完善的错误处理和用户提示

## 技术栈

### 前端
- Angular 17
- TypeScript
- Reactive Forms
- HTTP Client

### 后端
- Spring Boot 3
- Spring Security
- JWT
- Maven

## API端点

- `POST /setup` - 用户注册
- `POST /login` - 用户登录
- `GET /profile` - 获取用户信息（需要认证）

## 认证流程

1. 用户通过 `/login` 端点登录
2. 后端验证凭据并生成JWT token
3. JWT token存储在HTTP-only cookie中
4. 前端自动在后续请求中包含cookie
5. 后端验证cookie中的token进行认证

## 错误处理

### 后端错误信息

后端会返回具体的错误信息，包括：

**注册错误：**
- 邮箱为空：`"You must input your register email."`
- 邮箱已注册：`"Email already registered."`
- 密码为空：`"You must input your password."`
- 公司名称为空：`"You must input your corporation name."`
- 密码强度不足：`"Password must be at least 8 characters long and include at least one uppercase letter, one lowercase letter, and one digit."`

**登录错误：**
- 邮箱为空：`"You must input your email."`
- 密码为空：`"You must input your password."`
- 账户不存在：`"Your account doesn't exist, please register"`
- 密码错误：`"Wrong password!"`

### 前端错误处理

- 统一的错误处理服务
- 用户友好的错误提示
- 网络错误处理
- HTTP状态码处理
- 视觉突出的错误显示

## 注意事项

### 后端需要添加的端点

前端期望后端提供以下端点，如果还没有实现，需要添加：

```java
@GetMapping("/profile")
public ResponseEntity<UserDTO> getProfile() {
    // 从JWT token中获取用户信息
    // 返回用户详细信息
}
```

### CORS配置

后端已配置CORS允许来自 `http://localhost:3000` 的请求，并启用了credentials支持。

### Cookie配置

JWT token存储在名为 `auth_token` 的HTTP-only cookie中，有效期为7天。

## 测试

详细的测试指南请参考：
- `test-frontend.md` - 前端功能测试
- `error-handling-test.md` - 错误处理测试 