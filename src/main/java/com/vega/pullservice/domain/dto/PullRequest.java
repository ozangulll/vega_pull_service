package com.vega.pullservice.domain.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class PullRequest {
    
    @NotBlank(message = "Repository ID is required")
    private String repositoryId;
    
    private String commitHash; // Optional - if not provided, pull latest
    private boolean forcePull = false; // Force pull even if local is newer
}




