package com.vega.pullservice.domain.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.time.LocalDateTime;

@Entity
@Table(name = "repository_sync")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EntityListeners(AuditingEntityListener.class)
public class RepositorySync {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "user_id", nullable = false)
    private Long userId;
    
    @Column(name = "repository_id", nullable = false)
    private String repositoryId;
    
    @Column(name = "last_sync_commit", length = 64)
    private String lastSyncCommit;
    
    @LastModifiedDate
    @Column(name = "last_sync_at")
    private LocalDateTime lastSyncAt;
    
    @Enumerated(EnumType.STRING)
    @Column(name = "sync_status")
    @Builder.Default
    private SyncStatus syncStatus = SyncStatus.SYNCED;
    
    public enum SyncStatus {
        SYNCED, OUT_OF_SYNC, CONFLICT, ERROR
    }
}




