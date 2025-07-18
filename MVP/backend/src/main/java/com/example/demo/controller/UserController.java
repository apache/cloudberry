package com.example.demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.cors.CorsConfigurationSource;

import com.example.demo.DTO.UserDTO;
import com.example.demo.DTO.UserDetailsDTO;
import com.example.demo.DTO.request.LoginRequest;
import com.example.demo.DTO.request.SetupRequest;
import com.example.demo.DTO.response.LoginResponse;
import com.example.demo.service.UserService;

import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;


@RestController
public class UserController {

    private final CorsConfigurationSource corsConfigurationSource;

    @Autowired
    private UserService userService;

    UserController(CorsConfigurationSource corsConfigurationSource) {
        this.corsConfigurationSource = corsConfigurationSource;
    }

    @PostMapping("/setup")
    public ResponseEntity<UserDTO> setup(@RequestBody SetupRequest setupRequest){
        UserDTO userDTO = userService.register(setupRequest);
        return ResponseEntity.ok(userDTO);

    }

    @PostMapping("/login")
    public ResponseEntity<?> login(@RequestBody LoginRequest loginRequest,
                                                HttpServletResponse response){
        LoginResponse loginResponse = userService.login(
        loginRequest
        );
        Cookie jwtCookie = new Cookie("auth_token", loginResponse.getToken());
        jwtCookie.setHttpOnly(true);
        jwtCookie.setSecure(true);
        jwtCookie.setPath("/");
        jwtCookie.setMaxAge(7*24*60*60);
        response.addCookie(jwtCookie);
        return ResponseEntity.ok("Login successful");
    }
    @GetMapping("/profile")
    public ResponseEntity<UserDTO> Profile(){
        UserDetailsDTO user = getCurrentUserInfo();
        UserDTO userDTO = new UserDTO(user.getEmail(), user.getCorpName());
        return ResponseEntity.ok(userDTO);

    }

    private UserDetailsDTO getCurrentUserInfo(){
        Object currentUser = SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        if(currentUser instanceof UserDetailsDTO){
            return (UserDetailsDTO)currentUser;
        }else{
            throw new RuntimeException("Unauthenticated user.");
        }
    }

    
    
}
