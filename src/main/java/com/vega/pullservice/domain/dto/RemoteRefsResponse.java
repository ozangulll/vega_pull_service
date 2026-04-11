package com.vega.pullservice.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Lightweight view of refs on HDFS (no object download). Used by CLI {@code vega status} to detect drift from remote.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RemoteRefsResponse {

    private String repositoryId;

    /** Branch name → full commit hash */
    @Builder.Default
    private Map<String, String> heads = new LinkedHashMap<>();

    /** e.g. refs/heads/master when HEAD is symbolic */
    private String symbolicHead;

    /** Resolved commit at HEAD */
    private String headCommit;
}
