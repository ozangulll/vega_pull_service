package com.vega.pullservice.domain.service;

import com.vega.pullservice.domain.dto.PullResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

@Service
@Slf4j
public class HdfsPullService {
    
    @Value("${hadoop.hdfs.uri}")
    private String hdfsUri;
    
    @Value("${hadoop.hdfs.base-path}")
    private String basePath;
    
    private FileSystem getFileSystem() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);
        return FileSystem.get(conf);
    }
    
    public PullResponse downloadRepository(Long userId, String repositoryId) throws IOException {
        String hdfsPath = String.format("%s/%d/%s", basePath, userId, repositoryId);
        
        try (FileSystem fs = getFileSystem()) {
            Path repoPath = new Path(hdfsPath);
            if (!fs.exists(repoPath)) {
                throw new IOException("Repository not found: " + repositoryId);
            }
            
            List<PullResponse.FileInfo> files = new ArrayList<>();
            int fileCount = 0;
            long totalSize = 0;
            
            // Read metadata file first
            String metadataContent = readMetadataFile(fs, hdfsPath);
            String repositoryName = extractRepositoryName(metadataContent);
            
            // List all files in the repository
            RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(repoPath, true);
            
            while (fileIterator.hasNext()) {
                LocatedFileStatus fileStatus = fileIterator.next();
                if (fileStatus.isFile() && !fileStatus.getPath().getName().equals(".vega-metadata")) {
                    String relativePath = fileStatus.getPath().toString().substring(hdfsPath.length() + 1);
                    
                    // Download and decompress file
                    byte[] compressedData = readFileFromHdfs(fs, fileStatus.getPath());
                    byte[] decompressedData = decompressData(compressedData);
                    String content = new String(decompressedData);
                    
                    PullResponse.FileInfo fileInfo = PullResponse.FileInfo.builder()
                            .path(relativePath)
                            .content(content)
                            .hash(calculateHash(content))
                            .size((long) decompressedData.length)
                            .type(determineFileType(relativePath))
                            .build();
                    
                    files.add(fileInfo);
                    fileCount++;
                    totalSize += decompressedData.length;
                    
                    log.info("Downloaded file: {} from HDFS path: {}", relativePath, fileStatus.getPath());
                }
            }
            
            return PullResponse.builder()
                    .repositoryId(repositoryId)
                    .repositoryName(repositoryName)
                    .hdfsPath(hdfsPath)
                    .fileCount(fileCount)
                    .totalSize(totalSize)
                    .files(files)
                    .build();
        }
    }
    
    public boolean repositoryExists(Long userId, String repositoryId) throws IOException {
        String hdfsPath = String.format("%s/%d/%s", basePath, userId, repositoryId);
        
        try (FileSystem fs = getFileSystem()) {
            return fs.exists(new Path(hdfsPath));
        }
    }
    
    public String getRepositoryInfo(Long userId, String repositoryId) throws IOException {
        String hdfsPath = String.format("%s/%d/%s", basePath, userId, repositoryId);
        
        try (FileSystem fs = getFileSystem()) {
            Path repoPath = new Path(hdfsPath);
            if (!fs.exists(repoPath)) {
                throw new IOException("Repository not found: " + repositoryId);
            }
            
            return readMetadataFile(fs, hdfsPath);
        }
    }
    
    public List<String> listUserRepositories(Long userId) throws IOException {
        String userPath = String.format("%s/%d", basePath, userId);
        List<String> repositories = new ArrayList<>();
        
        try (FileSystem fs = getFileSystem()) {
            Path userDir = new Path(userPath);
            if (!fs.exists(userDir)) {
                return repositories;
            }
            
            RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(userDir, false);
            while (fileIterator.hasNext()) {
                LocatedFileStatus fileStatus = fileIterator.next();
                if (fileStatus.isDirectory()) {
                    String repoId = fileStatus.getPath().getName();
                    repositories.add(repoId);
                }
            }
        }
        
        return repositories;
    }
    
    private byte[] readFileFromHdfs(FileSystem fs, Path filePath) throws IOException {
        try (FSDataInputStream inputStream = fs.open(filePath);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }
            return baos.toByteArray();
        }
    }
    
    private byte[] decompressData(byte[] compressedData) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
             GZIPInputStream gzipIn = new GZIPInputStream(bais);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = gzipIn.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }
            return baos.toByteArray();
        }
    }
    
    private String readMetadataFile(FileSystem fs, String hdfsPath) throws IOException {
        Path metadataPath = new Path(hdfsPath + "/.vega-metadata");
        if (!fs.exists(metadataPath)) {
            return "";
        }
        
        try (FSDataInputStream inputStream = fs.open(metadataPath);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }
            return baos.toString();
        }
    }
    
    private String extractRepositoryName(String metadataContent) {
        for (String line : metadataContent.split("\n")) {
            if (line.startsWith("repository_name=")) {
                return line.substring("repository_name=".length());
            }
        }
        return "Unknown Repository";
    }
    
    private String calculateHash(String content) {
        return String.valueOf(content.hashCode());
    }
    
    private String determineFileType(String path) {
        if (path.endsWith(".vega")) {
            return "COMMIT";
        } else if (path.contains("/")) {
            return "TREE";
        } else {
            return "BLOB";
        }
    }
}




