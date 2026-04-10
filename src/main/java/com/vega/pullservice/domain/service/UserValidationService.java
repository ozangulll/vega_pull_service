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

@Service
@RequiredArgsConstructor
@Slf4j
public class UserValidationService {
    
    private final RestTemplate restTemplate;
    
    @Value("${user-service.url}")
    private String userServiceUrl;
    
    /**
     * Token'ı User Service'e göndererek validate eder. POST /api/auth/validate endpoint'ine istek gönderir.
     * Giriş: token (JWT token string)
     * Çıktı: Token geçerliyse true, değilse false
     * 
     * @param token JWT token string
     * @return Token geçerliyse true, değilse false
     */
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
    
    /**
     * Token'dan user ID'yi çıkarır. User Service'in /api/auth/user-id endpoint'ine POST isteği gönderir.
     * Giriş: token (JWT token string)
     * Çıktı: User ID (Long) veya null (token geçersizse veya kullanıcı bulunamazsa)
     * 
     * @param token JWT token string
     * @return User ID veya null
     */
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
    
    /**
     * Token'dan username'i çıkarır. User Service'in /api/auth/username endpoint'ine POST isteği gönderir.
     * Repository ID parse etmek için kullanılır (username/repository-name formatı).
     * Giriş: token (JWT token string)
     * Çıktı: Username string veya null (token geçersizse)
     * 
     * @param token JWT token string
     * @return Username veya null
     */
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




