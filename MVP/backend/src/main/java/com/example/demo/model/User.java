package com.example.demo.model;

import jakarta.persistence.*;;;
@Entity
@Table(name = "users")
public class User{
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "user_id")
    private Long userId;
    @Column(name = "email")
    private String email;
    @Column(name = "corp_name")
    private String corpName;
    @Column(name = "password_hash")
    private String passwordHash;

    public String getCorpName() {
        return corpName;
    }
    public String getEmail() {
        return email;
    }
    public Long getUserId() {
        return userId;
    }
    public String getPasswordHash() {
        return passwordHash;
    }
    public void setCorpName(String corpName) {
        this.corpName = corpName;
    }
    public void setEmail(String email) {
        this.email = email;
    }
    public void setPasswordHash(String passwordHash) {
        this.passwordHash = passwordHash;
    }
    public void setUserId(Long userId) {
        this.userId = userId;
    }

}