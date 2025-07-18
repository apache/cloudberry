package com.example.demo.DTO;

public class UserDTO {
    private String email;
    private String corpName;

    public UserDTO(String email, String corpName){
        this.email = email;
        this.corpName = corpName;
    }

    public String getCorpName() {
        return corpName;
    }
    public String getEmail() {
        return email;
    }
    public void setCorpName(String corpName) {
        this.corpName = corpName;
    }
    public void setEmail(String email) {
        this.email = email;
    }
    
}
