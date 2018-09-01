package org.cloud.cloud_manager.storage.manager.impl;

import org.cloud.aws_adapter.storage.impl.StorageServiceAwsImpl;
import org.cloud.azure_adapter.storage.impl.StorageServiceAzureImpl;
import org.cloud.cloud_manager.storage.StorageService;
import org.cloud.cloud_manager.storage.manager.StorageManagerService;
import org.cloud.cloud_manager.utils.CloudProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by shpilb on 9/1/2018.
 */

@Service
public class StorageManagerServiceImpl implements StorageManagerService {

    @Autowired
    private StorageServiceAwsImpl storageServiceAws;

    @Autowired
    private StorageServiceAzureImpl storageServiceAzure;

    private Map<CloudProvider, StorageService> storageServices;

    @PostConstruct
    private void init(){
        storageServices = new HashMap() {{
            put(CloudProvider.AZURE, storageServiceAzure);
            put(CloudProvider.AWS, storageServiceAws);
        }};
    }

    @Override
    public StorageService getStorageService(CloudProvider cloudProvider) {
        return storageServices.get(cloudProvider);
    }
}
