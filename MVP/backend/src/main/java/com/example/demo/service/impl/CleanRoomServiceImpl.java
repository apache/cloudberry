package com.example.demo.service.impl;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.RuntimeErrorException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

import com.example.demo.DTO.UserDTO;
import com.example.demo.DTO.UserDetailsDTO;
import com.example.demo.DTO.request.CleanRoomRequestDTO;
import com.example.demo.enums.CleanRoomErrorEnum;
import com.example.demo.enums.CleanRoomUserEnum;
import com.example.demo.exception.CleanRoomException;
import com.example.demo.exception.GlobalExceptionHandler;
import com.example.demo.model.CleanRoom;
import com.example.demo.model.User;
import com.example.demo.model.UserCleanroom;
import com.example.demo.repository.CleanRoomRepository;
import com.example.demo.repository.UserCleanRoomRepository;
import com.example.demo.service.CleanRoomService;

import jakarta.transaction.Transactional;
@Service
public class CleanRoomServiceImpl implements CleanRoomService{
    @Autowired
    private CleanRoomRepository cleanRoomRepository;

    @Autowired
    private UserCleanRoomRepository userCleanRoomRepository;

    @Override
    @Transactional
    public Map<String, Object> createCleanRoom(CleanRoomRequestDTO cleanRoomDTO) {
        Map<String, Object> result = new HashMap<>();
        if(cleanRoomDTO.getCleanRoomName() == null || cleanRoomDTO.getCleanRoomName().isEmpty()){
            throw new CleanRoomException(CleanRoomErrorEnum.NULL_CEANROOM_NAME);
        }
        UserDetailsDTO currentUser = (UserDetailsDTO)SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        User user = new User();
        user.setUserId(currentUser.getUserId());

        CleanRoom cleanRoom = new CleanRoom();
        try{
            cleanRoom.setRoomName(cleanRoomDTO.getCleanRoomName());
            if(cleanRoomDTO.getDescription() != null || !cleanRoomDTO.getDescription().isEmpty()){
                cleanRoom.setRoomDescription(cleanRoomDTO.getDescription());
            }
            cleanRoom.setCreatedAt(LocalDateTime.now());
            cleanRoomRepository.save(cleanRoom);

            UserCleanroom userCleanroom = new UserCleanroom();
            userCleanroom.setUser(user);
            userCleanroom.setCleanRoom(cleanRoom);
            userCleanroom.setUserStatus(CleanRoomUserEnum.PROVIDER);
            userCleanRoomRepository.save(userCleanroom);

            result.put("status", "success");
            result.put("cleanroom", cleanRoom);
            
            return result;
        }catch(CleanRoomException e){
            throw new CleanRoomException(CleanRoomErrorEnum.CLEANROOM_CREATION_FAILED);

        }catch(RuntimeException e){
            throw new RuntimeException(e.getMessage());
        }
        
        
        
    }
    @Override
    public Map<String, List<CleanRoom>> showAllCleanRoom(){
        UserDetailsDTO currentUser = (UserDetailsDTO)SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        if(currentUser == null){
            throw new CleanRoomException(CleanRoomErrorEnum.UNAUTHORIZED_ACCESS);
        }

        List<CleanRoom> created = new ArrayList<>();
        List<CleanRoom> joined = new ArrayList<>();

        List<UserCleanroom> allRooms = userCleanRoomRepository.findByUserId(currentUser.getUserId());

        for(UserCleanroom room : allRooms){
            if(room.getUserStatus() == CleanRoomUserEnum.CONSUMER){
                joined.add(cleanRoomRepository.findCleanRoomById(room.getCleanRoom().getRoomId()));
            }else if(room.getUserStatus() == CleanRoomUserEnum.PROVIDER){
                created.add(cleanRoomRepository.findCleanRoomById(room.getCleanRoom().getRoomId()));
            }
        }

        Map<String, List<CleanRoom>> result = new HashMap<>();
        result.put("created", created);
        result.put("joined", joined);
        return result;
        
    }

    

    
}
