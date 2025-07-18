package com.example.demo.enums;

public enum TableStatusEnum {
    IN_USE(0, "the table is still in use"),
    DELETED(1, "the table has been deleted");

    private final int code;
    private final String description;

    TableStatusEnum(int code, String description){
        this.code = code;
        this.description = description;
    }
    public int getCode() {
        return code;
    }
    public String getDescription() {
        return description;
    }
    
}
