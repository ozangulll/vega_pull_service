package com.vega.pullservice.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PullResponse {
    
    private Long pullId;
    private String repositoryId;
    private String repositoryName;
    private String hdfsPath;
    private String status;
    private Integer fileCount;
    private Long totalSize;
    private LocalDateTime createdAt;
    private String message;
    private List<FileInfo> files;
    
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FileInfo {
        private String path;
        private String content;
        private String hash;
        private Long size;
        private String type; // BLOB, TREE, COMMIT
    }
}




