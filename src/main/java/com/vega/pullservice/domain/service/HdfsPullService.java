package com.vega.pullservice.domain.service;

import com.vega.pullservice.domain.dto.PullResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
        // Disable permission checks for testing
        conf.set("dfs.permissions.enabled", "false");
        // Set user to root via UserGroupInformation
        try {
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser("root");
            ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.SIMPLE);
            return ugi.doAs((PrivilegedExceptionAction<FileSystem>) () -> {
                return FileSystem.get(conf);
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Failed to get FileSystem as root user", e);
        }
    }
    
    // Overloaded method for backward compatibility
    public PullResponse downloadRepository(Long userId, String repositoryId) throws IOException {
        // For backward compatibility - extract username if repositoryId is in format username/repo-name
        String hdfsPath;
        if (repositoryId.contains("/")) {
            // Repository ID is in format username/repository-name
            hdfsPath = String.format("%s/%s", basePath, repositoryId);
        } else {
            // Old format: use userId
            hdfsPath = String.format("%s/%d/%s", basePath, userId, repositoryId);
        }
        return downloadRepositoryInternal(hdfsPath, repositoryId);
    }
    
    /**
     * Repository'yi HDFS'ten indirir. Repository ID formatı: username/repository-name
     * HDFS path: /vega/repositories/username/repository-name
     * Tüm dosyaları HDFS'ten okur, GZIP ile decompress eder, Base64 encode eder ve PullResponse'a ekler.
     * Metadata dosyasını okur ve repository name'i çıkarır.
     * Giriş: username, repositoryName
     * Çıktı: PullResponse (repositoryId, repositoryName, hdfsPath, fileCount, totalSize, files array - her dosya path, content (Base64), hash, size, type içerir)
     * 
     * @param username Kullanıcı adı
     * @param repositoryName Repository ismi
     * @return PullResponse (dosyalar Base64 encode edilmiş)
     * @throws IOException Repository bulunamazsa, HDFS bağlantı hatası veya dosya okuma hatası olursa fırlatılır
     */
    public PullResponse downloadRepository(String username, String repositoryName) throws IOException {
        String hdfsPath = String.format("%s/%s/%s", basePath, username, repositoryName);
        String repositoryId = username + "/" + repositoryName;
        return downloadRepositoryInternal(hdfsPath, repositoryId);
    }
    
    private PullResponse downloadRepositoryInternal(String hdfsPath, String repositoryId) throws IOException {
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
                    Path filePath = fileStatus.getPath();
                    String fullPathString = filePath.toString();
                    // Extract relative path from full HDFS path
                    // Full path: hdfs://localhost:9000/vega/repositories/1/repo-id/path
                    // We want: path (relative to repo-id directory)
                    String relativePath;
                    
                    // Remove hdfs://namenode:9000 prefix if present
                    String normalizedPath = fullPathString;
                    if (normalizedPath.startsWith("hdfs://")) {
                        int thirdSlash = normalizedPath.indexOf("/", 7); // After hdfs://
                        if (thirdSlash > 0) {
                            normalizedPath = normalizedPath.substring(thirdSlash);
                        }
                    }
                    
                    // Extract relative path after hdfsPath
                    String hdfsPathNormalized = hdfsPath;
                    if (hdfsPathNormalized.startsWith("/")) {
                        hdfsPathNormalized = hdfsPathNormalized.substring(1);
                    }
                    if (!normalizedPath.startsWith("/")) {
                        normalizedPath = "/" + normalizedPath;
                    }
                    
                    if (normalizedPath.startsWith("/" + hdfsPathNormalized)) {
                        relativePath = normalizedPath.substring(hdfsPathNormalized.length() + 2); // +2 for // after hdfsPath
                    } else if (normalizedPath.contains(hdfsPathNormalized)) {
                        int idx = normalizedPath.indexOf(hdfsPathNormalized);
                        relativePath = normalizedPath.substring(idx + hdfsPathNormalized.length() + 1);
                    } else {
                        // Fallback: try Path.relativize
                        try {
                            Path repoPathObj = new Path(hdfsPath);
                            relativePath = repoPathObj.toUri().relativize(filePath.toUri()).getPath();
                            if (relativePath.startsWith("/")) {
                                relativePath = relativePath.substring(1);
                            }
                        } catch (Exception e) {
                            // Last resort: use filename
                            relativePath = filePath.getName();
                        }
                    }
                    
                    // Download and decompress file
                    byte[] compressedData = readFileFromHdfs(fs, fileStatus.getPath());
                    byte[] decompressedData = decompressData(compressedData);
                    // Encode content as Base64 for JSON transmission
                    String contentBase64 = java.util.Base64.getEncoder().encodeToString(decompressedData);
                    
                    PullResponse.FileInfo fileInfo = PullResponse.FileInfo.builder()
                            .path(relativePath)
                            .content(contentBase64)
                            .hash(calculateHash(new String(decompressedData)))
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
    
    // Overloaded method for backward compatibility
    public boolean repositoryExists(Long userId, String repositoryId) throws IOException {
        // Check if repositoryId is in format username/repository-name
        if (repositoryId.contains("/")) {
            String hdfsPath = String.format("%s/%s", basePath, repositoryId);
            try (FileSystem fs = getFileSystem()) {
                return fs.exists(new Path(hdfsPath));
            }
        } else {
            // Old format: use userId
            String hdfsPath = String.format("%s/%d/%s", basePath, userId, repositoryId);
            try (FileSystem fs = getFileSystem()) {
                return fs.exists(new Path(hdfsPath));
            }
        }
    }
    
    /**
     * Repository'nin HDFS'te var olup olmadığını kontrol eder. Repository ID formatı: username/repository-name
     * HDFS path: /vega/repositories/username/repository-name
     * Giriş: username, repositoryName
     * Çıktı: Repository varsa true, yoksa false
     * 
     * @param username Kullanıcı adı
     * @param repositoryName Repository ismi
     * @return Repository varsa true, yoksa false
     * @throws IOException HDFS bağlantı hatası olursa fırlatılır
     */
    public boolean repositoryExists(String username, String repositoryName) throws IOException {
        String hdfsPath = String.format("%s/%s/%s", basePath, username, repositoryName);
        try (FileSystem fs = getFileSystem()) {
            return fs.exists(new Path(hdfsPath));
        }
    }

    /**
     * Reads refs from HDFS without downloading objects. Paths match push layout: {@code HEAD}, {@code refs/heads/*}.
     */
    public static class RemoteRefsSnapshot {
        public final Map<String, String> heads;
        public final String symbolicHead;
        public final String headCommit;

        public RemoteRefsSnapshot(Map<String, String> heads, String symbolicHead, String headCommit) {
            this.heads = heads;
            this.symbolicHead = symbolicHead;
            this.headCommit = headCommit;
        }
    }

    public RemoteRefsSnapshot readRemoteRefsSnapshot(String username, String repositoryName) throws IOException {
        String hdfsPath = String.format("%s/%s/%s", basePath, username, repositoryName);
        try (FileSystem fs = getFileSystem()) {
            Path root = new Path(hdfsPath);
            if (!fs.exists(root)) {
                throw new IOException("Repository not found: " + username + "/" + repositoryName);
            }
            Map<String, String> heads = new LinkedHashMap<>();
            Path headsDir = new Path(hdfsPath + "/refs/heads");
            if (fs.exists(headsDir)) {
                FileStatus[] listed = fs.listStatus(headsDir);
                for (FileStatus st : listed) {
                    if (st.isFile()) {
                        String branch = st.getPath().getName();
                        readRepoTextFile(fs, hdfsPath, "refs/heads/" + branch).ifPresent(h -> heads.put(branch, h));
                    }
                }
            }
            String symbolicHead = null;
            String headCommit = null;
            Optional<String> headFile = readRepoTextFile(fs, hdfsPath, "HEAD");
            if (headFile.isPresent()) {
                String hc = headFile.get().trim();
                if (hc.startsWith("ref: ")) {
                    symbolicHead = hc.substring(5).trim();
                    headCommit = readRepoTextFile(fs, hdfsPath, symbolicHead).orElse(null);
                } else {
                    headCommit = hc;
                }
            }
            return new RemoteRefsSnapshot(heads, symbolicHead, headCommit);
        }
    }

    private Optional<String> readRepoTextFile(FileSystem fs, String hdfsRepoPath, String relativePath) throws IOException {
        Path p = new Path(hdfsRepoPath + "/" + relativePath.replace("\\", "/"));
        if (!fs.exists(p)) {
            return Optional.empty();
        }
        FileStatus st = fs.getFileStatus(p);
        if (!st.isFile()) {
            return Optional.empty();
        }
        byte[] compressed = readFileFromHdfs(fs, p);
        byte[] data = decompressData(compressed);
        return Optional.of(new String(data, StandardCharsets.UTF_8).trim());
    }

    /**
     * Lists repositories under {@code basePath/username/*} (current layout) and legacy {@code basePath/userId/*}.
     * Returns canonical ids {@code username/repo} for username layout, plain repo name for legacy dirs.
     */
    public List<String> listRepositoriesForUser(Long userId, String username) throws IOException {
        Set<String> ordered = new LinkedHashSet<>();
        if (username != null && !username.isBlank()) {
            String userPath = String.format("%s/%s", basePath, username);
            try (FileSystem fs = getFileSystem()) {
                Path userDir = new Path(userPath);
                if (fs.exists(userDir)) {
                    for (FileStatus st : fs.listStatus(userDir)) {
                        if (st.isDirectory()) {
                            String repo = st.getPath().getName();
                            ordered.add(username + "/" + repo);
                        }
                    }
                }
            }
        }
        String legacyPath = String.format("%s/%d", basePath, userId);
        try (FileSystem fs = getFileSystem()) {
            Path userDir = new Path(legacyPath);
            if (fs.exists(userDir)) {
                for (FileStatus st : fs.listStatus(userDir)) {
                    if (st.isDirectory()) {
                        ordered.add(st.getPath().getName());
                    }
                }
            }
        }
        return new ArrayList<>(ordered);
    }
    
    public String getRepositoryInfo(Long userId, String repositoryId) throws IOException {
        String hdfsPath;
        // Check if repositoryId is in format username/repository-name
        if (repositoryId.contains("/")) {
            hdfsPath = String.format("%s/%s", basePath, repositoryId);
        } else {
            // Old format: use userId
            hdfsPath = String.format("%s/%d/%s", basePath, userId, repositoryId);
        }
        
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
            return baos.toString(StandardCharsets.UTF_8);
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




