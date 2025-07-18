package com.example.demo.exception;

import com.example.demo.enums.ErrorEnum;

public class UploadException extends RuntimeException{
    private final ErrorEnum errorEnum;

    public UploadException(ErrorEnum errorEnum){
        super(errorEnum.getMessage());
        this.errorEnum = errorEnum;
    }
    public int getCode(){
        return errorEnum.getCode();
    }

    public String getMessage(){
        return errorEnum.getMessage();
    }

    
}
