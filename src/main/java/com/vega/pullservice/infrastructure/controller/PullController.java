package com.vega.pullservice.infrastructure.controller;

import com.vega.pullservice.domain.dto.PullRequest;
import com.vega.pullservice.domain.dto.PullResponse;
import com.vega.pullservice.domain.dto.RemoteRefsResponse;
import com.vega.pullservice.domain.service.PullService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/pull")
@RequiredArgsConstructor
public class PullController {
    
    private final PullService pullService;
    
    /**
     * Pull repository endpoint'i. POST /api/pull/repository
     * Authorization header'ından token'ı alır, PullService.pullRepository() metodunu çağırır.
     * Giriş: Authorization header (Bearer token), PullRequest (repositoryId, commitHash (opsiyonel), forcePull)
     * Çıktı: 200 OK ile PullResponse (dosyalar Base64 encode edilmiş) veya 400 Bad Request (hata durumunda)
     * 
     * @param authHeader Authorization header (Bearer prefix'i otomatik kaldırılır)
     * @param request PullRequest (repositoryId, commitHash (opsiyonel), forcePull)
     * @return ResponseEntity<PullResponse> (200 OK veya 400 Bad Request)
     */
    @PostMapping("/repository")
    public ResponseEntity<PullResponse> pullRepository(
            @RequestHeader("Authorization") String authHeader,
            @Valid @RequestBody PullRequest request) {
        try {
            // Remove "Bearer " prefix if present
            String token = authHeader;
            if (token != null && token.startsWith("Bearer ")) {
                token = token.substring(7);
            }
            
            PullResponse response = pullService.pullRepository(token, request);
            return ResponseEntity.ok(response);
        } catch (RuntimeException e) {
            e.printStackTrace();
            return ResponseEntity.badRequest()
                    .body(PullResponse.builder()
                            .status("FAILED")
                            .message("Pull failed: " + e.getMessage())
                            .build());
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.badRequest()
                    .body(PullResponse.builder()
                            .status("FAILED")
                            .message("Pull failed: " + e.getMessage())
                            .build());
        }
    }

    /**
     * Lightweight remote refs (HDFS HEAD + refs/heads/*) for CLI status — no object download.
     */
    @GetMapping("/repository/refs")
    public ResponseEntity<RemoteRefsResponse> getRepositoryRefs(
            @RequestHeader("Authorization") String authHeader,
            @RequestParam("repositoryId") String repositoryId) {
        try {
            String token = authHeader;
            if (token != null && token.startsWith("Bearer ")) {
                token = token.substring(7);
            }
            RemoteRefsResponse refs = pullService.getRemoteRefs(token, repositoryId);
            return ResponseEntity.ok(refs);
        } catch (RuntimeException e) {
            e.printStackTrace();
            return ResponseEntity.badRequest().build();
        }
    }
    
    @GetMapping("/history")
    public ResponseEntity<List<PullResponse>> getPullHistory(
            @RequestHeader("Authorization") String token) {
        try {
            List<PullResponse> history = pullService.getPullHistory(token);
            return ResponseEntity.ok(history);
        } catch (RuntimeException e) {
            return ResponseEntity.badRequest().build();
        }
    }
    
    @GetMapping("/status/{pullId}")
    public ResponseEntity<PullResponse> getPullStatus(
            @RequestHeader("Authorization") String token,
            @PathVariable Long pullId) {
        try {
            PullResponse status = pullService.getPullStatus(token, pullId);
            return ResponseEntity.ok(status);
        } catch (RuntimeException e) {
            return ResponseEntity.notFound().build();
        }
    }
    
    @PostMapping("/sync")
    public ResponseEntity<PullResponse> syncRepository(
            @RequestHeader("Authorization") String token,
            @RequestParam String repositoryId) {
        try {
            PullResponse response = pullService.syncRepository(token, repositoryId);
            return ResponseEntity.ok(response);
        } catch (RuntimeException e) {
            return ResponseEntity.badRequest().build();
        }
    }
}

@RestController
@RequestMapping("/api/repositories")
@RequiredArgsConstructor
class RepositoryController {
    
    private final PullService pullService;
    
    @GetMapping("/available")
    public ResponseEntity<List<String>> getAvailableRepositories(
            @RequestHeader("Authorization") String token) {
        try {
            List<String> repositories = pullService.getAvailableRepositories(token);
            return ResponseEntity.ok(repositories);
        } catch (RuntimeException e) {
            return ResponseEntity.badRequest().build();
        }
    }
    
    @GetMapping("/{repositoryId}/info")
    public ResponseEntity<String> getRepositoryInfo(
            @RequestHeader("Authorization") String token,
            @PathVariable String repositoryId) {
        try {
            String info = pullService.getRepositoryInfo(token, repositoryId);
            return ResponseEntity.ok(info);
        } catch (RuntimeException e) {
            return ResponseEntity.notFound().build();
        }
    }
}




