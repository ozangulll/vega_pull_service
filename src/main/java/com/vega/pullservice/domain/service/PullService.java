package com.vega.pullservice.domain.service;

import com.vega.pullservice.domain.dto.PullRequest;
import com.vega.pullservice.domain.dto.PullResponse;
import com.vega.pullservice.domain.model.PullOperation;
import com.vega.pullservice.domain.model.RepositorySync;
import com.vega.pullservice.domain.repository.PullOperationRepository;
import com.vega.pullservice.domain.repository.RepositorySyncRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class PullService {
    
    private final HdfsPullService hdfsPullService;
    private final UserValidationService userValidationService;
    private final PullOperationRepository pullOperationRepository;
    private final RepositorySyncRepository repositorySyncRepository;
    
    @Transactional
    public PullResponse pullRepository(String token, PullRequest request) {
        // Validate user token
        if (!userValidationService.validateToken(token)) {
            throw new RuntimeException("Invalid or expired token");
        }
        
        Long userId = userValidationService.getUserIdFromToken(token);
        if (userId == null) {
            throw new RuntimeException("Unable to determine user ID");
        }
        
        // Repository ID is now in format: username/repository-name
        // Extract username and repository name from repositoryId
        String username = null;
        String repositoryName = request.getRepositoryId();
        if (request.getRepositoryId().contains("/")) {
            String[] parts = request.getRepositoryId().split("/", 2);
            if (parts.length == 2) {
                username = parts[0];
                repositoryName = parts[1];
            }
        }
        
        // If username not in repositoryId, get from token
        if (username == null || username.isEmpty()) {
            username = userValidationService.getUsernameFromToken(token);
            if (username == null || username.isEmpty()) {
                // If repositoryId doesn't contain "/", assume it's just repository name and use current user
                username = userValidationService.getUsernameFromToken(token);
                if (username == null || username.isEmpty()) {
                    throw new RuntimeException("Unable to determine username");
                }
            }
        }
        
        // Repository ID in format: username/repository-name
        String repositoryId = username + "/" + repositoryName;
        
        // Check if repository exists in HDFS
        try {
            if (!hdfsPullService.repositoryExists(username, repositoryName)) {
                throw new RuntimeException("Repository not found: " + repositoryId);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error checking repository existence: " + e.getMessage());
        }
        
        // Create pull operation record
        PullOperation pullOperation = PullOperation.builder()
                .userId(userId)
                .repositoryId(repositoryId)
                .repositoryName(repositoryName)
                .hdfsPath("") // Will be updated after download
                .status(PullOperation.Status.PENDING)
                .createdAt(LocalDateTime.now()) // Set manually for builder
                .build();
        
        pullOperation = pullOperationRepository.save(pullOperation);
        
        try {
            // Update status to in progress
            pullOperation.setStatus(PullOperation.Status.IN_PROGRESS);
            pullOperationRepository.save(pullOperation);
            
            // Download from HDFS using username/repository-name format
            PullResponse pullResponse = hdfsPullService.downloadRepository(username, repositoryName);
            
            // Update pull operation with results
            pullOperation.setRepositoryName(pullResponse.getRepositoryName());
            pullOperation.setHdfsPath(pullResponse.getHdfsPath());
            pullOperation.setFileCount(pullResponse.getFileCount());
            pullOperation.setTotalSize(pullResponse.getTotalSize());
            pullOperation.setStatus(PullOperation.Status.COMPLETED);
            pullOperation.setCompletedAt(LocalDateTime.now());
            pullOperationRepository.save(pullOperation);
            
            // Update or create repository sync record
            updateRepositorySync(userId, request.getRepositoryId(), request.getCommitHash());
            
            log.info("Successfully pulled repository: {} for user: {}", request.getRepositoryId(), userId);
            
            return PullResponse.builder()
                    .pullId(pullOperation.getId())
                    .repositoryId(pullResponse.getRepositoryId())
                    .repositoryName(pullResponse.getRepositoryName())
                    .hdfsPath(pullResponse.getHdfsPath())
                    .status(pullOperation.getStatus().name())
                    .fileCount(pullResponse.getFileCount())
                    .totalSize(pullResponse.getTotalSize())
                    .createdAt(pullOperation.getCreatedAt())
                    .message("Repository pulled successfully")
                    .files(pullResponse.getFiles())
                    .build();
                    
        } catch (Exception e) {
            // Mark as failed
            pullOperation.setStatus(PullOperation.Status.FAILED);
            pullOperationRepository.save(pullOperation);
            
            log.error("Failed to pull repository: {} for user: {}", request.getRepositoryId(), userId, e);
            throw new RuntimeException("Failed to pull repository: " + e.getMessage());
        }
    }
    
    public List<PullResponse> getPullHistory(String token) {
        // Validate user token
        if (!userValidationService.validateToken(token)) {
            throw new RuntimeException("Invalid or expired token");
        }
        
        Long userId = userValidationService.getUserIdFromToken(token);
        if (userId == null) {
            throw new RuntimeException("Unable to determine user ID");
        }
        
        List<PullOperation> operations = pullOperationRepository.findByUserIdOrderByCreatedAtDesc(userId);
        
        return operations.stream()
                .map(this::mapToPullResponse)
                .toList();
    }
    
    public PullResponse getPullStatus(String token, Long pullId) {
        // Validate user token
        if (!userValidationService.validateToken(token)) {
            throw new RuntimeException("Invalid or expired token");
        }
        
        Long userId = userValidationService.getUserIdFromToken(token);
        if (userId == null) {
            throw new RuntimeException("Unable to determine user ID");
        }
        
        PullOperation operation = pullOperationRepository.findByIdAndUserId(pullId, userId)
                .orElseThrow(() -> new RuntimeException("Pull operation not found"));
        
        return mapToPullResponse(operation);
    }
    
    public List<String> getAvailableRepositories(String token) {
        // Validate user token
        if (!userValidationService.validateToken(token)) {
            throw new RuntimeException("Invalid or expired token");
        }
        
        Long userId = userValidationService.getUserIdFromToken(token);
        if (userId == null) {
            throw new RuntimeException("Unable to determine user ID");
        }
        
        try {
            return hdfsPullService.listUserRepositories(userId);
        } catch (Exception e) {
            log.error("Failed to list repositories for user: {}", userId, e);
            throw new RuntimeException("Failed to list repositories: " + e.getMessage());
        }
    }
    
    public String getRepositoryInfo(String token, String repositoryId) {
        // Validate user token
        if (!userValidationService.validateToken(token)) {
            throw new RuntimeException("Invalid or expired token");
        }
        
        Long userId = userValidationService.getUserIdFromToken(token);
        if (userId == null) {
            throw new RuntimeException("Unable to determine user ID");
        }
        
        try {
            return hdfsPullService.getRepositoryInfo(userId, repositoryId);
        } catch (Exception e) {
            log.error("Failed to get repository info for: {} user: {}", repositoryId, userId, e);
            throw new RuntimeException("Failed to get repository info: " + e.getMessage());
        }
    }
    
    @Transactional
    public PullResponse syncRepository(String token, String repositoryId) {
        // This would sync with the latest changes from HDFS
        PullRequest syncRequest = new PullRequest();
        syncRequest.setRepositoryId(repositoryId);
        syncRequest.setForcePull(true);
        
        return pullRepository(token, syncRequest);
    }
    
    private void updateRepositorySync(Long userId, String repositoryId, String commitHash) {
        RepositorySync existingSync = repositorySyncRepository
                .findByUserIdAndRepositoryId(userId, repositoryId)
                .orElse(null);
        
        if (existingSync != null) {
            // Update existing sync record
            existingSync.setLastSyncCommit(commitHash);
            existingSync.setSyncStatus(RepositorySync.SyncStatus.SYNCED);
            repositorySyncRepository.save(existingSync);
        } else {
            // Create new sync record
            RepositorySync sync = RepositorySync.builder()
                    .userId(userId)
                    .repositoryId(repositoryId)
                    .lastSyncCommit(commitHash)
                    .syncStatus(RepositorySync.SyncStatus.SYNCED)
                    .build();
            repositorySyncRepository.save(sync);
        }
    }
    
    private PullResponse mapToPullResponse(PullOperation operation) {
        return PullResponse.builder()
                .pullId(operation.getId())
                .repositoryId(operation.getRepositoryId())
                .repositoryName(operation.getRepositoryName())
                .hdfsPath(operation.getHdfsPath())
                .status(operation.getStatus().name())
                .fileCount(operation.getFileCount())
                .totalSize(operation.getTotalSize())
                .createdAt(operation.getCreatedAt())
                .message(operation.getStatus() == PullOperation.Status.COMPLETED ? 
                    "Repository pulled successfully" : 
                    "Pull operation " + operation.getStatus().name().toLowerCase())
                .build();
    }
}




