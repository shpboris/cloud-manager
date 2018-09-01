package org.cloud.cloud_manager.storage.manager;

import org.cloud.cloud_manager.storage.StorageService;
import org.cloud.cloud_manager.utils.CloudProvider;

/**
 * Created by shpilb on 9/1/2018.
 */
public interface StorageManagerService {
    public StorageService getStorageService(CloudProvider cloudProvider);
}
