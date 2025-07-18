package com.example.demo.enums;

public enum CleanRoomUserEnum {
    PROVIDER(0, "provider of shared data"),
    CONSUMER(1, "consumer of shared data");

    private final int code;
    private final String message;

    CleanRoomUserEnum(int code, String message){
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
