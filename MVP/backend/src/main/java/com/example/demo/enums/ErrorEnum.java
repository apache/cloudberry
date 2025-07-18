package com.example.demo.enums;

public enum ErrorEnum {
    SUCCESS(0, "success"),


    INVALID_PARAM(1001, "invalid param"),
    UNAUTHORIZED(1002, "not logged in or login has expired"),
    FORBIDDEN(1003, "No access"),

    FILE_EMPTY(2001, "The uploaded file is empty"),
    FILE_NAME_MISSING(2002, "file name missing"),
    FILE_TYPE_ERROR(2003, "File type not supported, only accept csv and json"),
    FILE_UPLOAD_FAIL(2004, "File upload failed"),

    NEGATIVE_ERROR_LIMIT(3001, "The error tolerance limit cannot be negative"),
    ERROR_LIMIT_NOT_NUMBER(3002, "The error tolerance limit must be a number");



    private final int code;
    private final String message;
    
    ErrorEnum(int code, String message){
        this.code = code;
        this.message = message;
    }
    
    public int getCode() {
        return code;
    }
    public String getMessage() {
        return message;
    }
    

    
    
}
