package com.example.demo.utils;

import java.util.Date;

import org.springframework.boot.autoconfigure.security.oauth2.resource.OAuth2ResourceServerProperties.Jwt;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;

import java.security.Key;

public class JwtUtils {
    private static final Key SECRET_KEY = Keys.secretKeyFor(SignatureAlgorithm.HS256);
    private static final long EXPIRATION_TIME = 3600_000;
    
    public static String generateToken(Long userId, String corpName, String email){
        return Jwts.builder()
        .setSubject(email)
        .claim("userId", userId)
        .claim("corpName", corpName)
        .setIssuedAt(new Date())
        .setExpiration(new Date(System.currentTimeMillis() + EXPIRATION_TIME))
        .signWith(SECRET_KEY)
        .compact();
    }

    public static Claims parseToken(String token) throws JwtException {
        return Jwts.parserBuilder()
                .setSigningKey(SECRET_KEY)
                .build()
                .parseClaimsJws(token)
                .getBody();
    }

    public static boolean validateToken(String token){
        try{
            parseToken(token);
            return true;
        }catch(Exception e){
            return false;
        }
    }

    public static Long getUserIdFromToken(String token){
        return parseToken(token).get("userId", Long.class);
    }

    public static String getCorpNameFromToken(String token){
        return parseToken(token).get("corpName", String.class);
    }

    public static String getEmailFromToken(String token){
        return parseToken(token).getSubject();
    }
    
}
