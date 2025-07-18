package com.example.demo.model;

import java.time.LocalDateTime;
import java.util.List;

import com.example.demo.utils.ListToJsonConverterUtil;

import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.MappedSuperclass;
@MappedSuperclass
public abstract class Policy {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    protected Long policyId;

    @Column(name = "function_type")
    protected String functionType;

    @Column(name = "targets")
    @Convert(converter = ListToJsonConverterUtil.class)
    protected List<String> targets;

    @Column(name = "created_at")
    protected LocalDateTime createdAt;

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }
    public String getFunctionType() {
        return functionType;
    }
    public Long getPolicyId() {
        return policyId;
    }
    public List<String> getTargets() {
        return targets;
    }
    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }
    public void setFunctionType(String functionType) {
        this.functionType = functionType;
    }
    public void setPolicyId(Long policyId) {
        this.policyId = policyId;
    }
    public void setTargets(List<String> targets) {
        this.targets = targets;
    }
}
