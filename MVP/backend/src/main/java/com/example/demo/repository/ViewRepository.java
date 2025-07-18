package com.example.demo.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.example.demo.model.View;

public interface ViewRepository extends JpaRepository<View, Long>{
    
}
