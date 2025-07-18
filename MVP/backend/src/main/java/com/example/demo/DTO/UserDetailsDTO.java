package com.example.demo.DTO;

public class UserDetailsDTO {
    private Long userId;
    private String corpName;
    private String email;

    public UserDetailsDTO(String email, String corpName, Long userId){
        this.email = email;
        this.corpName = corpName;
        this.userId = userId;
    }

    public String getCorpName() {
        return corpName;
    }
    public String getEmail() {
        return email;
    }
    public Long getUserId() {
        return userId;
    }

    
}
