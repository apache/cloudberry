package com.example.demo.repository;

import java.util.List;

import org.hibernate.type.descriptor.converter.spi.JpaAttributeConverter;
import org.springframework.data.jpa.repository.JpaRepository;

import com.example.demo.model.UserCleanroom;

public interface UserCleanRoomRepository extends JpaRepository<UserCleanroom, Long>{

    List<UserCleanroom> findByUserId(Long userId);
}
