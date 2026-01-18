package com.vega.pullservice.domain.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class UserValidationService {
    
    private final RestTemplate restTemplate;
    
    @Value("${user-service.url}")
    private String userServiceUrl;
    
    public boolean validateToken(String token) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("Authorization", "Bearer " + token);
            HttpEntity<String> entity = new HttpEntity<>(headers);
            
            String url = userServiceUrl + "/api/auth/validate";
            ResponseEntity<Boolean> response = restTemplate.exchange(
                url, HttpMethod.POST, entity, Boolean.class
            );
            
            return response.getBody() != null && response.getBody();
        } catch (Exception e) {
            log.error("Token validation failed: {}", e.getMessage());
            return false;
        }
    }
    
    public Long getUserIdFromToken(String token) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("Authorization", "Bearer " + token);
            HttpEntity<String> entity = new HttpEntity<>(headers);
            
            String url = userServiceUrl + "/api/auth/user-id";
            ResponseEntity<Long> response = restTemplate.exchange(
                url, HttpMethod.POST, entity, Long.class
            );
            
            if (response.getBody() != null) {
                return response.getBody();
            }
        } catch (Exception e) {
            log.error("Failed to get user ID from token: {}", e.getMessage());
        }
        return null;
    }
    
    public String getUsernameFromToken(String token) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("Authorization", "Bearer " + token);
            HttpEntity<String> entity = new HttpEntity<>(headers);
            
            String url = userServiceUrl + "/api/auth/username";
            ResponseEntity<String> response = restTemplate.exchange(
                url, HttpMethod.POST, entity, String.class
            );
            
            if (response.getBody() != null && !response.getBody().isEmpty()) {
                return response.getBody();
            }
        } catch (Exception e) {
            log.error("Failed to get username from token: {}", e.getMessage());
        }
        return null;
    }
}




