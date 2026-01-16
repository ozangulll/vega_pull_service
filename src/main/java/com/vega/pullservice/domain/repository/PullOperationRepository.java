package com.vega.pullservice.domain.repository;

import com.vega.pullservice.domain.model.PullOperation;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface PullOperationRepository extends JpaRepository<PullOperation, Long> {
    
    List<PullOperation> findByUserIdOrderByCreatedAtDesc(Long userId);
    
    Optional<PullOperation> findByIdAndUserId(Long id, Long userId);
    
    List<PullOperation> findByRepositoryIdAndUserId(String repositoryId, Long userId);
}




