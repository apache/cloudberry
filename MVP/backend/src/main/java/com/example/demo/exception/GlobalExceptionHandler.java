package com.example.demo.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import com.example.demo.DTO.response.ApiResponse;
import com.example.demo.enums.CleanRoomErrorEnum;

import java.util.HashMap;
import java.util.Map;

@ControllerAdvice
public class GlobalExceptionHandler {
    
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> handleIllegalArgumentException(IllegalArgumentException e) {
        Map<String, String> errorResponse = new HashMap<>();
        errorResponse.put("message", e.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
    }
    
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, String>> handleGenericException(Exception e) {
        Map<String, String> errorResponse = new HashMap<>();
        errorResponse.put("message", "An unexpected error occurred: " + e.getMessage());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
    }
    @ExceptionHandler(UploadException.class)
    public ResponseEntity<ApiResponse<?>> handleUploadException(UploadException e){
        return ResponseEntity.badRequest().body(new ApiResponse<>(e.getCode(), e.getMessage(), null));
    }
    @ExceptionHandler(JsonConversionException.class)
    public ResponseEntity<Map<String, String>> handleJsonConversionException(JsonConversionException e){
        Map<String, String> errorResponse = new HashMap<>();
        errorResponse.put("message", "opration failed, please try again");
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
    }
    @ExceptionHandler(CleanRoomException.class)
    public ResponseEntity<Map<String, String>> handleCleanRoomException(CleanRoomException e){
        CleanRoomErrorEnum error = e.getError();
        Map<String, String> errorResponse = new HashMap<>();
        if (error.getExposedToClient()) {
        errorResponse.put("code", String.valueOf(error.getCode()));
        errorResponse.put("message", error.getMessage());
        return ResponseEntity.badRequest().body(errorResponse);
        } else {
            errorResponse.put("code", "5000");
            errorResponse.put("message", "Unexpected internal error");
            System.err.println("Internal error: " + error.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

}
