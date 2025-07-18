package com.example.demo.exception;

import com.example.demo.enums.CleanRoomErrorEnum;

public class CleanRoomException extends RuntimeException{
    private final CleanRoomErrorEnum error;

    public CleanRoomException(CleanRoomErrorEnum error) {
        super(error.getMessage());
        this.error = error;
    }
    public CleanRoomErrorEnum getError() {
        return error;
    }
    
}
