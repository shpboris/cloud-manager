package org.cloud.azure_adapter.storage.impl;

import com.microsoft.azure.AzureEnvironment;
import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.azure.management.Azure;
import com.microsoft.azure.management.storage.StorageAccount;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.blob.*;
import lombok.extern.slf4j.Slf4j;
import org.cloud.cloud_manager.storage.StorageService;
import org.springframework.stereotype.Service;

import java.io.File;
import java.time.Instant;

/**
 * Created by shpilb on 9/1/2018.
 */

@Service
@Slf4j
public class StorageServiceAzureImpl implements StorageService {

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
        StorageAccount storageAccount = getAzureClient().storageAccounts().list()
                .stream()
                .filter(s -> s.name().equals(storageAccountName))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Failed to find storage account"));

        CloudStorageAccount cloudStorageAccount = CloudStorageAccount.parse(getStorageConnectionString(storageAccount));
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

    private String getStorageConnectionString(StorageAccount storageAccount){
        String storageConnectionString =
                "DefaultEndpointsProtocol=http;"
                        + "AccountName=%s;"
                        + "AccountKey=%s";
        storageConnectionString =
                String.format(storageConnectionString, storageAccount.name(),
                        storageAccount.getKeys().get(0).value());
        return storageConnectionString;
    }
}
