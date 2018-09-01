package org.cloud.cloud_manager.storage;

/**
 * Created by shpilb on 9/1/2018.
 */
public interface StorageService {
    public void uploadFile(String cloudStorageName, String cloudContainerName, String localFileName, String cloudFileName);
    public void downloadFile(String cloudStorageName, String cloudContainerName, String localFileName, String cloudFileName);
}
