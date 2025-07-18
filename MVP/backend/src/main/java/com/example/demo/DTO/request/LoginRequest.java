package com.example.demo.DTO.request;

public class LoginRequest {
    private String email;
    private String rawPass;

    public LoginRequest(String email, String rawPass){
        this.email = email;
        this.rawPass = rawPass;
    }
    public String getEmail() {
        return email;
    }
    public String getRawPass() {
        return rawPass;
    }
    public void setEmail(String email) {
        this.email = email;
    }
    public void setRawPass(String rawPass) {
        this.rawPass = rawPass;
    }
    
}
