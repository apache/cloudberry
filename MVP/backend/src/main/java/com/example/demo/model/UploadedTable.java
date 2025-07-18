package com.example.demo.model;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
@Entity
@Table(name = "uploaded_table")
public class UploadedTable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long uploadId;

    @Column(name = "table_name")
    private String tableName;

    @ManyToOne
    @JoinColumn(name = "user_id")
    private User user;

    @Column(name = "upload_time")
    private LocalDateTime uploadTime;

    @Column(name = "original_file_name")
    private String originalFileName;

    @Column(name = "status")
    private int status = 0;

    @Column(name = "description")
    private String description;

    public String getDescription() {
        return description;
    }

    public String getOriginalFileName() {
        return originalFileName;
    }
    public int getStatus() {
        return status;
    }
    public String getTableName() {
        return tableName;
    }
    public Long getUploadId() {
        return uploadId;
    }
    public LocalDateTime getUploadTime() {
        return uploadTime;
    }
    public User getUser() {
        return user;
    }
    public void setDescription(String description) {
        this.description = description;
    }
    public void setOriginalFileName(String originalFileName) {
        this.originalFileName = originalFileName;
    }
    public void setStatus(int status) {
        this.status = status;
    }
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    public void setUploadId(Long uploadId) {
        this.uploadId = uploadId;
    }
    public void setUploadTime(LocalDateTime uploadTime) {
        this.uploadTime = uploadTime;
    }
    public void setUser(User user) {
        this.user = user;
    }
    
}
