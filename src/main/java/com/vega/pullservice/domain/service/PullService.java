package com.vega.pullservice.domain.service;

import com.vega.pullservice.domain.dto.PullRequest;
import com.vega.pullservice.domain.dto.PullResponse;
import com.vega.pullservice.domain.dto.RemoteRefsResponse;
import com.vega.pullservice.domain.model.PullOperation;
import com.vega.pullservice.domain.model.RepositorySync;
import com.vega.pullservice.domain.repository.PullOperationRepository;
import com.vega.pullservice.domain.repository.RepositorySyncRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class PullService {
    
    private final HdfsPullService hdfsPullService;
    private final UserValidationService userValidationService;
    private final PullOperationRepository pullOperationRepository;
    private final RepositorySyncRepository repositorySyncRepository;
    private final RepoPullAccessClient repoPullAccessClient;
    
    /**
     * Repository'yi HDFS'ten pull eder. Token'ı validate eder, repository ID'den username ve repository name'i çıkarır,
     * HDFS'te repository'nin var olup olmadığını kontrol eder, PullOperation kaydı oluşturur,
     * HdfsPullService ile repository'yi HDFS'ten indirir, RepositorySync kaydı günceller.
     * Repository ID formatı: "repository-name" veya "username/repository-name" olabilir.
     * Giriş: token (JWT token), request (repositoryId, commitHash (opsiyonel), forcePull)
     * Çıktı: PullResponse (pullId, repositoryId, repositoryName, hdfsPath, status, fileCount, totalSize, createdAt, message, files array)
     * 
     * @param token JWT token (Authorization header'dan gelir)
     * @param request PullRequest (repositoryId, commitHash (opsiyonel), forcePull)
     * @return PullResponse (pull işlemi sonucu ve dosyalar)
     * @throws RuntimeException Token geçersizse, repository bulunamazsa veya HDFS download hatası olursa fırlatılır
     */
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

        PullTarget target = resolvePullTarget(request.getRepositoryId(), token);
        String username = target.owner();
        String repositoryName = target.repo();
        String repositoryId = target.canonical();

        repoPullAccessClient.assertCanPull(token, username, repositoryName);

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
            
            String syncCommit = request.getCommitHash();
            if (syncCommit == null || syncCommit.isEmpty()) {
                try {
                    HdfsPullService.RemoteRefsSnapshot snap = hdfsPullService.readRemoteRefsSnapshot(username, repositoryName);
                    if (snap.headCommit != null && !snap.headCommit.isEmpty()) {
                        syncCommit = snap.headCommit;
                    }
                } catch (Exception e) {
                    log.debug("Could not read remote HEAD for sync metadata: {}", e.getMessage());
                }
            }
            updateRepositorySync(userId, repositoryId, syncCommit);
            
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
            String username = userValidationService.getUsernameFromToken(token);
            return hdfsPullService.listRepositoriesForUser(userId, username);
        } catch (Exception e) {
            log.error("Failed to list repositories for user: {}", userId, e);
            throw new RuntimeException("Failed to list repositories: " + e.getMessage());
        }
    }

    /**
     * Reads branch tips and HEAD from HDFS only (no blob download). For CLI remote status.
     */
    public RemoteRefsResponse getRemoteRefs(String token, String repositoryIdParam) {
        if (!userValidationService.validateToken(token)) {
            throw new RuntimeException("Invalid or expired token");
        }
        Long userId = userValidationService.getUserIdFromToken(token);
        if (userId == null) {
            throw new RuntimeException("Unable to determine user ID");
        }

        PullTarget target = resolvePullTarget(repositoryIdParam, token);
        String username = target.owner();
        String repositoryName = target.repo();
        String canonical = target.canonical();

        repoPullAccessClient.assertCanPull(token, username, repositoryName);

        try {
            if (!hdfsPullService.repositoryExists(username, repositoryName)) {
                throw new RuntimeException("Repository not found: " + canonical);
            }
            HdfsPullService.RemoteRefsSnapshot snap = hdfsPullService.readRemoteRefsSnapshot(username, repositoryName);
            return RemoteRefsResponse.builder()
                    .repositoryId(canonical)
                    .heads(new LinkedHashMap<>(snap.heads))
                    .symbolicHead(snap.symbolicHead)
                    .headCommit(snap.headCommit)
                    .build();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read remote refs: " + e.getMessage());
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

        if (repositoryId != null && repositoryId.contains("/")) {
            PullTarget target = resolvePullTarget(repositoryId, token);
            repoPullAccessClient.assertCanPull(token, target.owner(), target.repo());
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
    
    private PullTarget resolvePullTarget(String repositoryIdParam, String token) {
        String owner = null;
        String repoName = repositoryIdParam;
        if (repositoryIdParam != null && repositoryIdParam.contains("/")) {
            String[] parts = repositoryIdParam.split("/", 2);
            if (parts.length == 2) {
                owner = parts[0];
                repoName = parts[1];
            }
        }
        if (owner == null || owner.isEmpty()) {
            owner = userValidationService.getUsernameFromToken(token);
            if (owner == null || owner.isEmpty()) {
                throw new RuntimeException("Unable to determine username");
            }
        }
        return new PullTarget(owner, repoName, owner + "/" + repoName);
    }

    private record PullTarget(String owner, String repo, String canonical) {}

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




