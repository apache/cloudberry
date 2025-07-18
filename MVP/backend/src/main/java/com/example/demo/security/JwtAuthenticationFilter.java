package com.example.demo.security;

import java.io.IOException;

import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.security.web.authentication.WebAuthenticationDetails;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import com.example.demo.DTO.UserDetailsDTO;
import com.example.demo.utils.JwtUtils;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.log4j.Log4j2;
@Log4j2
@Component
public class JwtAuthenticationFilter extends OncePerRequestFilter{

    /**
     *
     */
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException{
        String jwt = null;
        if(request.getCookies() != null){
            for (Cookie cookie : request.getCookies()) {
                if ("auth_token".equals(cookie.getName())) {
                    jwt = cookie.getValue();
                    break;
                }
            }
        }

        if(jwt != null && JwtUtils.validateToken(jwt)){
            Long userId = JwtUtils.getUserIdFromToken(jwt);
            String corpName = JwtUtils.getCorpNameFromToken(jwt);
            String email = JwtUtils.getEmailFromToken(jwt);

            /**
             *set token to UsernamePasswordAuthenticationToken
             *all services can get user info from SecurityContext
             */
            UserDetailsDTO userDtailsDTO = new UserDetailsDTO(email, corpName, userId);
            UsernamePasswordAuthenticationToken auth = new UsernamePasswordAuthenticationToken(userDtailsDTO, null, null);
            auth.setDetails(new WebAuthenticationDetailsSource().buildDetails(request));
            SecurityContextHolder.getContext().setAuthentication(auth);
        }
        filterChain.doFilter(request, response);
    }
    
}
