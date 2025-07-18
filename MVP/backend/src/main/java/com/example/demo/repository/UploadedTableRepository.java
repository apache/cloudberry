package com.example.demo.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.example.demo.model.UploadedTable;

public interface UploadedTableRepository extends JpaRepository<UploadedTable, Long>{

    
} 
