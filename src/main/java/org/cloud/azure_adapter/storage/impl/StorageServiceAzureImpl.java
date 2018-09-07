package org.cloud.azure_adapter.storage.impl;

import com.microsoft.azure.AzureEnvironment;
import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.azure.management.Azure;
import com.microsoft.azure.management.storage.StorageAccount;
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.cloud.cloud_manager.storage.StorageService;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.time.Instant;
import java.util.*;

/**
 * Created by shpilb on 9/1/2018.
 */

@Service
@Slf4j
public class StorageServiceAzureImpl implements StorageService {

    private static final int cacheEvictionSeconds = 600;
    private Pair<Instant, List<StorageAccount>> storageAccountsCache = null;
    private Map<String, Pair<Instant, String>> storageAccountToAccessKeysCache = new HashMap<>();

    @Override
    public void uploadFile(String cloudStorageName, String cloudContainerName, String localFileName, String cloudFileName) {
        try {
            log.debug("Started upload to Azure at: {}", Instant.now());
            CloudBlockBlob blob = getBlobReference(cloudStorageName, cloudContainerName, cloudFileName);
            String localFilePath = System.getProperty("localDirPath") + File.separator + localFileName;
            log.debug("Started upload file: {} to storage account: {} , container is: {}", localFilePath, cloudStorageName, cloudContainerName);
            blob.uploadFromFile(localFilePath);
            log.debug("Completed upload file: {} to storage account: {} , container is: {}", localFilePath, cloudStorageName, cloudContainerName);
            log.debug("Completed upload to Azure at: {}", Instant.now());
        } catch (Exception e){
            String errMsg = String.format("Failed to upload file %s", cloudFileName);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public void downloadFile(String cloudStorageName, String cloudContainerName, String localFileName, String cloudFileName) {
        try {
            log.debug("Started download from Azure at: {}", Instant.now());
            CloudBlockBlob blob = getBlobReference(cloudStorageName, cloudContainerName, cloudFileName);
            String localFilePath = System.getProperty("localDirPath") + File.separator + localFileName;
            log.debug("Started download file: {} from storage account: {} , container is: {}", localFilePath, cloudStorageName, cloudContainerName);
            blob.downloadToFile(localFilePath);
            log.debug("Completed download file: {} from storage account: {} , container is: {}", localFilePath, cloudStorageName, cloudContainerName);
            log.debug("Completed download from Azure at: {}", Instant.now());
        } catch (Exception e){
            String errMsg = String.format("Failed to download file %s", cloudFileName);
            throw new RuntimeException(errMsg, e);
        }
    }

    private CloudBlockBlob getBlobReference(String storageAccountName, String containerName, String targetFileName) throws Exception{
        log.debug("Started getting blob reference to file: {} in storage account: {} , container is: {}" ,
                targetFileName, storageAccountName, containerName);
        StorageAccount storageAccount = getStorageAccount(storageAccountName);
        CloudStorageAccount cloudStorageAccount = getCloudStorageAccount(storageAccount);
        CloudBlobClient blobClient = cloudStorageAccount.createCloudBlobClient();
        CloudBlobContainer container = blobClient.getContainerReference(containerName);
        BlobRequestOptions options = new BlobRequestOptions();
        options.setConcurrentRequestCount(20);
        OperationContext operationContext = new OperationContext();
        operationContext.setLoggingEnabled(true);
        container.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, options, operationContext);
        CloudBlockBlob blob = container.getBlockBlobReference(targetFileName);
        log.debug("Completed getting blob reference to file: {} in storage account: {} , container is: {}" ,
                targetFileName, storageAccountName, containerName);
        return blob;
    }

    private Azure getAzureClient() {
        String domain = System.getProperty("domain");
        String subscription = System.getProperty("subscription");
        String client = System.getProperty("client");
        String secret = System.getProperty("secret");
        ApplicationTokenCredentials credentials = new ApplicationTokenCredentials(
                client,
                domain,
                secret, AzureEnvironment.AZURE);
        Azure azure = Azure.authenticate(credentials).withSubscription(subscription);
        return azure;
    }

    private StorageAccount getStorageAccount(String storageAccountName){
        StorageAccount res = null;
        if(isAccountsCacheUpdateRequired(storageAccountName)){
            List<StorageAccount> storageAccounts = getAzureClient().storageAccounts().list();
            storageAccountsCache = Pair.of(Instant.now(), storageAccounts);
        }
        if(!CollectionUtils.isEmpty(storageAccountsCache.getRight())){
            res = storageAccountsCache
                    .getRight()
                    .stream()
                    .filter(sa -> sa.name().equals(storageAccountName))
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("Storage account wasn't found"));
        }
        return res;
    }

    private CloudStorageAccount getCloudStorageAccount(StorageAccount storageAccount) throws Exception {
        CloudStorageAccount cloudStorageAccount = null;
        String key = null;
        if(isKeysCacheUpdateRequired(storageAccount.name())){
            key = storageAccount.getKeys().get(0).value();
            storageAccountToAccessKeysCache.put(storageAccount.name(), Pair.of(Instant.now(), key));
        } else {
            key = storageAccountToAccessKeysCache.get(storageAccount.name()).getRight();
        }
        cloudStorageAccount = new CloudStorageAccount(new StorageCredentialsAccountAndKey(storageAccount.name(), key), true);
        return cloudStorageAccount;
    }

    private boolean isAccountsCacheUpdateRequired(String storageAccountName){
        boolean res = false;
        if(storageAccountsCache == null
                || CollectionUtils.isEmpty(storageAccountsCache.getRight())
                || storageAccountsCache.getLeft().isBefore(Instant.now().minusSeconds(cacheEvictionSeconds))
                || !storageAccountsCache.getRight().stream().anyMatch(sa -> sa.name().equals(storageAccountName))){
            log.debug("Searching for account: {}, update of storage accounts cache is required", storageAccountName);
            res = true;
        }
        return res;
    }

    private boolean isKeysCacheUpdateRequired(String storageAccountName){
        boolean res = false;
        if(storageAccountToAccessKeysCache == null
                || storageAccountToAccessKeysCache.get(storageAccountName) == null
                || storageAccountToAccessKeysCache.get(storageAccountName)
                        .getLeft().isBefore(Instant.now().minusSeconds(cacheEvictionSeconds))){
            log.debug("Searching for account: {}, update of storage accounts keys cache is required", storageAccountName);
            res = true;
        }
        return res;
    }
}
