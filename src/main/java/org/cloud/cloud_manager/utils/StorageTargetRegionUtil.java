package org.cloud.cloud_manager.utils;

/**
 * Created by shpilb on 9/1/2018.
 */
public class StorageTargetRegionUtil {
    private static String targetRegion;

    public static String getTargetRegion() {
        return targetRegion;
    }

    public static void setTargetRegion(String targetRegion) {
        StorageTargetRegionUtil.targetRegion = targetRegion;
    }
}
