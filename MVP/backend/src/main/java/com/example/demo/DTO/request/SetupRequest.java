package com.example.demo.DTO.request;

public class SetupRequest {
    private String email;
    private String rawPass;
    private String corpName;

    public SetupRequest(String email, String rawPass, String corpName){
        this.corpName = corpName;
        this.email = email;
        this.rawPass = rawPass;
    }
    public String getCorpName() {
        return corpName;
    }
    public String getEmail() {
        return email;
    }
    public String getRawPass() {
        return rawPass;
    }
    public void setCorpName(String corpName) {
        this.corpName = corpName;
    }
    public void setEmail(String email) {
        this.email = email;
    }
    public void setRawPass(String rawPass) {
        this.rawPass = rawPass;
    }

    
}
