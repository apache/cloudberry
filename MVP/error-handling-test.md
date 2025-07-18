# 错误处理测试指南

## 后端错误信息

后端会返回以下具体的错误信息：

### 注册错误 (Setup)
- `"You must input your register email."` - 邮箱为空
- `"Email already registered."` - 邮箱已注册
- `"You must input your password."` - 密码为空
- `"You must input your corporation name."` - 公司名称为空
- `"Password must be at least 8 characters long and include at least one uppercase letter, one lowercase letter, and one digit."` - 密码强度不足

### 登录错误 (Login)
- `"You must input your email."` - 邮箱为空
- `"You must input your password."` - 密码为空
- `"Your account doesn't exist, please register"` - 账户不存在
- `"Wrong password!"` - 密码错误

## 前端错误处理

### 错误显示
- 错误信息会显示在表单下方
- 使用红色背景和边框突出显示
- 添加了轻微的抖动动画效果
- 每次提交表单时会清除之前的错误信息

### 网络错误
- 连接失败：`"无法连接到服务器，请检查网络连接"`
- 服务器错误：`"服务器内部错误，请稍后重试"`
- 网关错误：`"网关错误，请稍后重试"`

### HTTP状态码处理
- 400：请求参数错误
- 401：未授权，需要重新登录
- 403：权限不足
- 404：资源不存在
- 409：资源冲突
- 422：数据验证失败
- 500：服务器内部错误
- 502：网关错误
- 503：服务暂时不可用

## 测试场景

### 1. 注册测试
1. **空邮箱测试**
   - 输入：邮箱为空，密码和公司名称正常
   - 预期：显示 "You must input your register email."

2. **已注册邮箱测试**
   - 输入：使用已注册的邮箱
   - 预期：显示 "Email already registered."

3. **空密码测试**
   - 输入：邮箱正常，密码为空，公司名称正常
   - 预期：显示 "You must input your password."

4. **空公司名称测试**
   - 输入：邮箱和密码正常，公司名称为空
   - 预期：显示 "You must input your corporation name."

5. **弱密码测试**
   - 输入：密码不符合强度要求（少于8位，或缺少大小写字母、数字）
   - 预期：显示密码强度要求的具体信息

### 2. 登录测试
1. **空邮箱测试**
   - 输入：邮箱为空，密码正常
   - 预期：显示 "You must input your email."

2. **空密码测试**
   - 输入：邮箱正常，密码为空
   - 预期：显示 "You must input your password."

3. **不存在的账户测试**
   - 输入：未注册的邮箱和任意密码
   - 预期：显示 "Your account doesn't exist, please register"

4. **错误密码测试**
   - 输入：正确邮箱，错误密码
   - 预期：显示 "Wrong password!"

### 3. 网络错误测试
1. **后端服务关闭**
   - 关闭后端服务
   - 尝试注册或登录
   - 预期：显示 "无法连接到服务器，请检查网络连接"

2. **网络断开**
   - 断开网络连接
   - 尝试注册或登录
   - 预期：显示 "无法连接到服务器，请检查网络连接"

## 错误信息样式

错误信息具有以下视觉特征：
- 红色文字 (#e74c3c)
- 浅红色背景 (#fdf2f2)
- 红色边框和左边框
- 圆角设计
- 轻微的抖动动画
- 居中对齐
- 适当的内边距

## 用户体验改进

1. **即时反馈**：错误信息在表单提交后立即显示
2. **清晰明确**：错误信息具体说明问题所在
3. **视觉突出**：使用颜色和动画吸引用户注意
4. **自动清除**：每次新的提交会清除之前的错误信息
5. **友好提示**：提供具体的解决建议 