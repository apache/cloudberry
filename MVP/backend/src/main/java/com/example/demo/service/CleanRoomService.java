package com.example.demo.service;

import java.util.List;
import java.util.Map;

import com.example.demo.DTO.UserDTO;
import com.example.demo.DTO.request.CleanRoomRequestDTO;
import com.example.demo.DTO.request.RowColumnPolicyRequestDTO;
import com.example.demo.model.CleanRoom;
import com.example.demo.model.User;
import com.example.demo.repository.UserCleanRoomRepository;

public interface CleanRoomService {
    Map<String, Object> createCleanRoom(CleanRoomRequestDTO cleanRoomDTO);

    Map<String, List<CleanRoom>> showAllCleanRoom();

    createRowColumnPolicy(RowColumnPolicyRequestDTO);

    createJoinPolicy(UserCleanRoom userCleanroom);

    createAggregationPolicy(UserCleanRoom userCleanroom);

    create
    
}
