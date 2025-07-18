package com.example.demo.DTO.response;

public class LoginResponse {
    private String token;
    private String email;
    private String corpName;

    public LoginResponse(String token, String email, String corpName){
        this.token = token;
        this.email = email;
        this.corpName = corpName;
    }
    public String getCorpName() {
        return corpName;
    }
    public String getEmail() {
        return email;
    }
    public String getToken() {
        return token;
    }
    public void setCorpName(String corpName) {
        this.corpName = corpName;
    }
    public void setEmail(String email) {
        this.email = email;
    }
    public void setToken(String token) {
        this.token = token;
    }
    
}
