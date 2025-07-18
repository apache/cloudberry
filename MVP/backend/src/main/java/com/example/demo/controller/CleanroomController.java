package com.example.demo.controller;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.DTO.request.CleanRoomRequestDTO;
import com.example.demo.model.CleanRoom;
import com.example.demo.service.CleanRoomService;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Slf4j
@RestController
public class CleanroomController {
    @Autowired
    private CleanRoomService cleanRoomService;

    @PostMapping("/create-room")
    public ResponseEntity<?>createCleanRoom(@RequestBody CleanRoomRequestDTO cleanRoomDTO){
        return ResponseEntity.ok(cleanRoomService.createCleanRoom(cleanRoomDTO));
    
    }
    
    @GetMapping("path")
    public ResponseEntity<?>showAllCleanRoom(){
        Map<String, List<CleanRoom>> result = cleanRoomService.showAllCleanRoom();
        return ResponseEntity.ok(result);
    }
}
