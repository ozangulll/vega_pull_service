package com.vega.pullservice.domain.repository;

import com.vega.pullservice.domain.model.RepositorySync;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface RepositorySyncRepository extends JpaRepository<RepositorySync, Long> {
    
    List<RepositorySync> findByUserIdOrderByLastSyncAtDesc(Long userId);
    
    Optional<RepositorySync> findByUserIdAndRepositoryId(Long userId, String repositoryId);
    
    List<RepositorySync> findByRepositoryId(String repositoryId);
}




