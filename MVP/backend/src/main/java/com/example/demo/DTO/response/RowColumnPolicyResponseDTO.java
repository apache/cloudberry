package com.example.demo.DTO.response;

import java.util.List;

import com.example.demo.model.ColumnRowPolicy;

public class RowColumnPolicyResponseDTO {
    private Long RowColumnPolicyId;
    private List<String> allowed;
    private Long createdBy;

    public RowColumnPolicyResponseDTO(Long RowColumnPolicyId, List<String> allowed, Long createdBy){
        this.RowColumnPolicyId = RowColumnPolicyId;
        this.allowed = allowed;
        this.createdBy = createdBy;
    
    }
    public List<String> getAllowed() {
        return allowed;
    }
    public Long getCreatedBy() {
        return createdBy;
    }
    public Long getRowColumnPolicyId() {
        return RowColumnPolicyId;
    }
    public void setAllowed(List<String> allowed) {
        this.allowed = allowed;
    }
    public void setCreatedBy(Long createdBy) {
        this.createdBy = createdBy;
    }
    public void setRowColumnPolicyId(Long rowColumnPolicyId) {
        RowColumnPolicyId = rowColumnPolicyId;
    }
}
