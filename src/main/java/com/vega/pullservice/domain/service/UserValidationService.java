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
            
            String url = userServiceUrl + "/api/users/profile";
            ResponseEntity<Map> response = restTemplate.exchange(
                url, HttpMethod.GET, entity, Map.class
            );
            
            if (response.getBody() != null) {
                Object userId = response.getBody().get("id");
                if (userId instanceof Number) {
                    return ((Number) userId).longValue();
                }
            }
        } catch (Exception e) {
            log.error("Failed to get user ID from token: {}", e.getMessage());
        }
        return null;
    }
}




