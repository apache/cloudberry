package com.example.demo.service;

import org.springframework.stereotype.Service;

import com.example.demo.DTO.UserDTO;
import com.example.demo.DTO.request.LoginRequest;
import com.example.demo.DTO.request.SetupRequest;
import com.example.demo.DTO.response.LoginResponse;
import com.example.demo.model.User;

import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;


public interface UserService {
    UserDTO register(SetupRequest setupRequest);
    LoginResponse login(LoginRequest loginRequest);
    
    
} 
