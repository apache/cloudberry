package com.example.demo.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.example.demo.model.CleanRoom;

public interface CleanRoomRepository extends JpaRepository<CleanRoom, Long>{
    List<CleanRoom> findAll();
    CleanRoom findCleanRoomById(Long roomId);
}
