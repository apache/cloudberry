package com.example.demo.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import jakarta.servlet.http.Cookie;

import com.example.demo.DTO.UserDTO;
import com.example.demo.DTO.request.LoginRequest;
import com.example.demo.DTO.request.SetupRequest;
import com.example.demo.DTO.response.LoginResponse;
import com.example.demo.model.User;
import com.example.demo.repository.UserRepository;
import com.example.demo.service.UserService;
import com.example.demo.utils.JwtUtils;
import com.example.demo.utils.PasswordUtils;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import jakarta.transaction.Transactional;

@Service
public class UserServiceImpl implements UserService{

    @Autowired
    private UserRepository userRepository;


    @Override
    @Transactional
    public UserDTO register(SetupRequest setupRequest){
        
        if (setupRequest.getEmail() == null || setupRequest.getEmail().isEmpty()){
            throw new IllegalArgumentException("You must input your register email.");
        }
        if(userRepository.existsByEmail(setupRequest.getEmail().trim())){
            throw new IllegalArgumentException("Email already registered.");
        }
        if(setupRequest.getRawPass() == null || setupRequest.getRawPass().isEmpty()){
            throw new IllegalArgumentException("You must input your password.");
        }
        if(setupRequest.getCorpName() == null || setupRequest.getCorpName().isEmpty()){
            throw new IllegalArgumentException("You must input your corporation name.");
        }
        if(!PasswordUtils.passStrength(setupRequest.getRawPass())){
            throw new IllegalArgumentException("Password must be at least 8 characters long and include at least one uppercase letter, one lowercase letter, and one digit.");
        }
        String encryptedPass = PasswordUtils.encodePass(setupRequest.getRawPass());
        User userStoreToDataBase = new User();
        userStoreToDataBase.setCorpName(setupRequest.getCorpName());
        userStoreToDataBase.setEmail(setupRequest.getEmail().trim());
        userStoreToDataBase.setPasswordHash(encryptedPass);
        userRepository.save(userStoreToDataBase);

        UserDTO userDTO = new UserDTO(setupRequest.getEmail(), setupRequest.getCorpName());
        return userDTO;
    }
    
    @Override
    public LoginResponse login(LoginRequest loginRequest){
        if(loginRequest.getEmail() == null || loginRequest.getEmail().isEmpty()){
            throw new IllegalArgumentException("You must input your email.");
        }
        if(loginRequest.getRawPass() == null || loginRequest.getRawPass().isEmpty()){
            throw new IllegalArgumentException("You must input your password.");
        }
        if(!userRepository.existsByEmail(loginRequest.getEmail())){
            throw new IllegalArgumentException("Your account doesn't exist, please register");
        }
        User user = userRepository.findUserByEmail(loginRequest.getEmail());
        
        if(!PasswordUtils.passMatches(loginRequest.getRawPass(), user.getPasswordHash())){
            throw new IllegalArgumentException("Wrong password!");
        }
        String token = JwtUtils.generateToken(user.getUserId(), user.getCorpName(), user.getEmail());
        return new LoginResponse(token, loginRequest.getEmail(), loginRequest.getEmail());
    }

    


}
