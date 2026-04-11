package com.vega.pullservice.domain.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;

/**
 * Asks Vega Repos whether the bearer token's user may read (pull) a repo.
 * Public → any authenticated user; private → owner or collaborator only (same as {@code RepoAccessService.canAccess}).
 */
@Service
@Slf4j
public class RepoPullAccessClient {

    private final RestTemplate restTemplate;

    @Value("${vega.repos-service.url:http://localhost:8086}")
    private String reposBaseUrl;

    @Value("${vega.repos-service.require-access-check:true}")
    private boolean requireAccessCheck;

    public RepoPullAccessClient(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public void assertCanPull(String rawToken, String ownerUsername, String repositoryName) {
        if (!requireAccessCheck) {
            log.warn("vega.repos-service.require-access-check=false — pull access not verified (dev only)");
            return;
        }
        String token = rawToken != null && rawToken.startsWith("Bearer ") ? rawToken.substring(7) : rawToken;
        if (token == null || token.isBlank()) {
            throw new RuntimeException("Missing token for repository access check");
        }

        URI uri = UriComponentsBuilder.fromHttpUrl(reposBaseUrl.trim())
                .path("/api/repos/{owner}/{repo}/pull-access")
                .buildAndExpand(ownerUsername, repositoryName)
                .encode()
                .toUri();

        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Bearer " + token);
        HttpEntity<Void> entity = new HttpEntity<>(headers);

        try {
            var response = restTemplate.exchange(uri, HttpMethod.GET, entity, Void.class);
            if (response.getStatusCode().is2xxSuccessful()) {
                return;
            }
            throw new RuntimeException("Repository access check failed: HTTP " + response.getStatusCode());
        } catch (HttpStatusCodeException e) {
            int code = e.getStatusCode().value();
            if (code == HttpStatus.FORBIDDEN.value()) {
                throw new RuntimeException(
                        "Access denied: private repository — you must be the owner or a collaborator to pull "
                                + ownerUsername + "/" + repositoryName);
            }
            if (code == HttpStatus.UNAUTHORIZED.value()) {
                throw new RuntimeException("Invalid or expired token");
            }
            if (code == HttpStatus.NOT_FOUND.value()) {
                throw new RuntimeException(
                        "Repository access API not found. Update Vega Repos backend and ensure it is running on "
                                + reposBaseUrl);
            }
            throw new RuntimeException("Repository access check failed: " + e.getStatusCode() + " " + e.getResponseBodyAsString());
        } catch (ResourceAccessException e) {
            log.error("Vega Repos unreachable at {}: {}", reposBaseUrl, e.getMessage());
            throw new RuntimeException(
                    "Cannot verify repository access (Vega Repos unavailable at " + reposBaseUrl
                            + "). Pull is blocked for security. Set vega.repos-service.require-access-check=false only for local dev without Repos.",
                    e);
        }
    }
}
