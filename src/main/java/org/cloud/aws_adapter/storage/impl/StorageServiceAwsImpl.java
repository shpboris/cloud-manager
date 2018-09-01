package org.cloud.aws_adapter.storage.impl;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.ExecutorFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.*;
import lombok.extern.slf4j.Slf4j;
import org.cloud.cloud_manager.storage.StorageService;
import org.cloud.cloud_manager.utils.StorageTargetRegionUtil;
import org.springframework.stereotype.Service;

import java.io.File;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by shpilb on 9/1/2018.
 */

@Service
@Slf4j
public class StorageServiceAwsImpl implements StorageService {

    public static final long FILE_MIN_SIZE_TO_USE_MULTI_PART = 20 * 1024 * 1024;
    public static final int MIN_PART_SIZE = 5 * 1024 * 1024;

    @Override
    public void uploadFile(String cloudStorageName, String cloudContainerName, String localFileName, String cloudFileName) {
        try {
            log.debug("Started upload to AWS at: {}", Instant.now());
            String localFilePath = System.getProperty("localDirPath") + File.separator + localFileName;
            File file = new File(localFilePath);
            TransferManager transferManager = null;

            try {
                transferManager = TransferManagerBuilder.standard()
                        .withS3Client(getAwsS3Client(StorageTargetRegionUtil.getTargetRegion()))
                        .withMultipartUploadThreshold(FILE_MIN_SIZE_TO_USE_MULTI_PART)
                        .withMinimumUploadPartSize(new Long(MIN_PART_SIZE))
                        .withExecutorFactory(new ExecutorFactory() {
                            @Override
                            public ExecutorService newExecutor() {
                                return Executors.newFixedThreadPool(5);
                            }
                        })
                        .build();

                PutObjectRequest putObjectRequest = new PutObjectRequest(cloudStorageName, cloudFileName, file);
                Upload upload = transferManager.upload(putObjectRequest);
                upload.waitForUploadResult();
            } finally {
                if (transferManager != null) {
                    transferManager.shutdownNow(false);
                }
            }
            log.debug("Completed upload to AWS at: {}", Instant.now());
        } catch (Exception e){
            String errMsg = String.format("Failed to upload file %s", cloudFileName);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public void downloadFile(String cloudStorageName, String cloudContainerName, String localFileName, String cloudFileName) {
        try {
            log.debug("Started AWS download test at: {}", Instant.now());
            TransferManager transferManager = null;

            try {
                transferManager = TransferManagerBuilder.standard()
                        .withS3Client(getAwsS3Client(StorageTargetRegionUtil.getTargetRegion()))
                        .build();
                String localFilePath = System.getProperty("localDirPath") + File.separator + localFileName;
                Download download = transferManager.download(cloudStorageName, cloudFileName, new File(localFilePath));
                download.waitForCompletion();
            } finally {
                if (transferManager != null) {
                    transferManager.shutdownNow(false);
                }
            }
            log.debug("Completed AWS download test at: {}", Instant.now());
        } catch (Exception e){
            String errMsg = String.format("Failed to download file %s", cloudFileName);
            throw new RuntimeException(errMsg, e);
        }
    }

    private AmazonS3 getAwsS3Client(String region){
        String accessKey = System.getProperty("accessKey");
        String secretKey = System.getProperty("secretKey");
        BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3 amazonS3Client = AmazonS3ClientBuilder.standard()
                .withRegion(region)
                .withCredentials(new AWSStaticCredentialsProvider(basicAWSCredentials))
                .build();
        return amazonS3Client;
    }
}
