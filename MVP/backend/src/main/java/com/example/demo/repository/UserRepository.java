package com.example.demo.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.example.demo.model.User;

public interface UserRepository extends JpaRepository<User, Long>{
    User findUserByEmail(String email);

    Boolean existsByEmail(String email);

}
