package com.vega.pullservice.domain.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.time.LocalDateTime;

@Entity
@Table(name = "pull_operations")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EntityListeners(AuditingEntityListener.class)
public class PullOperation {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "user_id", nullable = false)
    private Long userId;
    
    @Column(name = "repository_id", nullable = false)
    private String repositoryId;
    
    @Column(name = "repository_name", nullable = false)
    private String repositoryName;
    
    @Column(name = "hdfs_path", nullable = false)
    private String hdfsPath;
    
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    @Builder.Default
    private Status status = Status.PENDING;
    
    @Column(name = "file_count")
    @Builder.Default
    private Integer fileCount = 0;
    
    @Column(name = "total_size")
    @Builder.Default
    private Long totalSize = 0L;
    
    @CreatedDate
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;
    
    @Column(name = "completed_at")
    private LocalDateTime completedAt;
    
    public enum Status {
        PENDING, IN_PROGRESS, COMPLETED, FAILED
    }
}




