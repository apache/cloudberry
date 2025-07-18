package com.example.demo.enums;

public enum CleanRoomErrorEnum {

    NULL_ARGUMENT(1001, "Argument cannot be null", true),
    INVALID_ARGUMENT(1002, "Invalid argument", true),
    UNAUTHORIZED_ACCESS(1003, "Unauthorized access to the CleanRoom", true),
    USER_NOT_FOUND(1004, "User not found", true),
    CLEANROOM_NOT_FOUND(1005, "CleanRoom not found", true),

    // CleanRoom creation
    NULL_CEANROOM_NAME(2001, "cleanroom name cannot be null", true),
    CLEANROOM_CREATION_FAILED(2002, "Failed to create CleanRoom", true),

    // User-related
    USER_ALREADY_JOINED(3001, "User has already joined the CleanRoom", true),
    USER_NOT_IN_CLEANROOM(3002, "User has not joined the CleanRoom", true),
    INVALID_USER_STATUS(3003, "Invalid user status", true),

    // Table/data related
    TABLE_ALREADY_IN(4001, "Table already in this clean room", true),
    INVALID_TABLE_SCHEMA(4002, "Invalid table schema", true),
    DATA_QUERY_FAILED(4003, "Failed to query data", true),

    // View and sharing
    VIEW_CREATION_FAILED(6001, "Failed to create view", false),
    INVALID_VIEW_DEFINITION(6002, "Invalid view definition", false),

    // System/service-level
    INTERNAL_SERVER_ERROR(9001, "Internal server error", true),
    DATABASE_CONNECTION_FAILED(9002, "Failed to connect to the database", false);

    private final int code;
    private final String message;
    private final boolean exposedToClient;

    CleanRoomErrorEnum(int code, String message, boolean exposedToClient){
        this.code = code;
        this.message = message;
        this.exposedToClient = exposedToClient;
    }

    public int getCode(){
        return code;
    }
    public String getMessage(){
        return message;
    }
    public boolean getExposedToClient(){
        return exposedToClient;
    }
    
}
