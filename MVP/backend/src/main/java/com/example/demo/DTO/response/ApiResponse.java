package com.example.demo.DTO.response;

import com.example.demo.enums.ErrorEnum;

public class ApiResponse<T> {
    private int code;
    private String message;
    private T data;

    public ApiResponse(){}

    public ApiResponse(int code, String message, T data){
        this.code = code;
        this.message = message;
        this.data = data;
    }

    public static <T> ApiResponse<T> success(T data){
        return new ApiResponse<>(ErrorEnum.SUCCESS.getCode(), ErrorEnum.SUCCESS.getMessage(), data);
    }

    public static <T> ApiResponse<T> error(ErrorEnum errorCode){
        return new ApiResponse<>(errorCode.getCode(), errorCode.getMessage(), null);
    }

    public static <T> ApiResponse<T> error(ErrorEnum errorCode, String customMessage) {
        return new ApiResponse<>(errorCode.getCode(), customMessage, null);
    }

    public int getCode() {
        return code;
    }
    public String getMessage() {
        return message;
    }
    public T getData() {
        return data;
    }
    public void setCode(int code) {
        this.code = code;
    }
    public void setData(T data) {
        this.data = data;
    }
    public void setMessage(String message) {
        this.message = message;
    }
    
    
}
