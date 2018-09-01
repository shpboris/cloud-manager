package org.cloud.resources;

import org.apache.commons.lang3.StringUtils;
import org.cloud.cloud_manager.storage.manager.StorageManagerService;
import org.cloud.cloud_manager.utils.CloudProvider;
import org.cloud.cloud_manager.utils.StorageTargetRegionUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

@RestController
public class StorageResource {

    @Autowired
    private StorageManagerService storageManagerService;

	@RequestMapping(value = "/storage/{cloudProvider}/upload/{localFileName}/{cloudStorage}",
			method = RequestMethod.PUT)
	public ResponseEntity<HttpStatus> upload(@PathVariable("cloudProvider") String cloudProvider,
						 @PathVariable("localFileName") String localFileName,
						 @PathVariable("cloudStorage") String cloudStorage,
						 @RequestParam(value = "cloudContainerName", required = false) String cloudContainerName,
	                     @RequestParam(value = "cloudFileName", required = false) String cloudFileName,
                         @RequestParam(value = "region", required = false) String region) {

        if(StringUtils.isEmpty(cloudFileName)){
            cloudFileName = localFileName;
        }
        if(StringUtils.isEmpty(cloudContainerName)
                && CloudProvider.valueOf(cloudProvider).equals(CloudProvider.AZURE)){
            throw new RuntimeException("Cloud container should not be empty for Azure");
        }
        if(StringUtils.isEmpty(region)
                && CloudProvider.valueOf(cloudProvider).equals(CloudProvider.AWS)){
            throw new RuntimeException("Region should not be empty for Aws");
        }
        StorageTargetRegionUtil.setTargetRegion(region);
        storageManagerService
                .getStorageService(CloudProvider.valueOf(cloudProvider))
                .uploadFile(cloudStorage, cloudContainerName, localFileName, cloudFileName);

        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
	}



    @RequestMapping(value = "/storage/{cloudProvider}/download/{cloudStorage}/{cloudFileName}",
            method = RequestMethod.PUT)
    public ResponseEntity<HttpStatus> download(@PathVariable("cloudProvider") String cloudProvider,
                                             @PathVariable("cloudStorage") String cloudStorage,
                                             @PathVariable("cloudFileName") String cloudFileName,
                                             @RequestParam(value = "localFileName", required = false) String localFileName,
                                             @RequestParam(value = "cloudContainerName", required = false) String cloudContainerName,
                                             @RequestParam(value = "region" , required = false) String region) {

        if(StringUtils.isEmpty(localFileName)){
            localFileName = cloudFileName;
        }
        if(StringUtils.isEmpty(cloudContainerName)
                && CloudProvider.valueOf(cloudProvider).equals(CloudProvider.AZURE)){
            throw new RuntimeException("Cloud container should not be empty for Azure");
        }
        if(StringUtils.isEmpty(region)
                && CloudProvider.valueOf(cloudProvider).equals(CloudProvider.AWS)){
            throw new RuntimeException("Region should not be empty for Aws");
        }
        StorageTargetRegionUtil.setTargetRegion(region);
        storageManagerService
                .getStorageService(CloudProvider.valueOf(cloudProvider))
                .downloadFile(cloudStorage, cloudContainerName, localFileName, cloudFileName);

        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
}