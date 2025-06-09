package nl.medtechchain.chaincode.service.query.average;

import com.google.protobuf.Descriptors;
import nl.medtechchain.chaincode.service.query.QueryProcessor;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

private static class AvgResult {
    final long sum;
    final int count;
    
    VersionResult(long sum, int count) {
        this.sum = sum;
        this.count = count;
    }
}

// Calculates avg of int fields and handles plain/encrypted data
public class AverageQuery extends QueryProcessor {
    
    public AverageQuery(PlatformConfig platformConfig) {
        super(platformConfig);
    }
    
    @Override
    public QueryResult process(Query query, List<DeviceDataAsset> assets) {
        var fieldDescriptor = getFieldDescriptor(query.getTargetField());

        Map<String, List<DeviceDataAsset>> versionGroups = groupByVersion(assets);
        
        long totalSum = 0;
        int totalCount = 0;
        
        for (Map.Entry<String, List<DeviceDataAsset>> entry : versionGroups.entrySet()) {
            String version = entry.getKey();
            List<DeviceDataAsset> versionAssets = entry.getValue();
            
            logger.fine("Processing average for " + versionAssets.size() + " assets with version: " + version);
            
            var result = processVersionGroup(versionAssets, fieldDescriptor, version);
            totalSum += result.sum;
            totalCount += result.count;
        }
        
        if (totalCount == 0) {
            logger.warning("No valid values");
            return QueryResult.newBuilder().setAverageResult(0.0).build();
        }
        
        double average = (double) totalSum / totalCount;
        logger.info("Average across all versions: " + average);
        return QueryResult.newBuilder().setAverageResult(average).build();
    }
    
  
    
    private AvgResult processVersionGroup(List<DeviceDataAsset> assets, 
                                            Descriptors.FieldDescriptor fieldDescriptor, 
                                            String version) {
        long plainSum = 0;
        int plainCount = 0;
        List<String> encryptedValues = new ArrayList<>();
        
        for (DeviceDataAsset asset : assets) {
            var fieldValue = (DeviceDataAsset.IntegerField) asset.getDeviceData().getField(fieldDescriptor);
            
            switch (fieldValue.getFieldCase()) {
                case PLAIN:
                    plainSum += fieldValue.getPlain();
                    plainCount++;
                    break;
                    
                case ENCRYPTED:
                    if (encryptionService == null) {
                        throw new IllegalStateException("Found encrypted data but no encryption service configured. " +
                            "Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME to 'paillier' or 'bfv'.");
                    }
                    if (encryptionService.isHomomorphic()) {
                        encryptedValues.add(fieldValue.getEncrypted());
                    } else {
                        plainSum += encryptionService.decryptLong(fieldValue.getEncrypted(), version);
                        plainCount++;
                    }
                    break;
                    
                case FIELD_NOT_SET:
                    logger.fine("Skipping asset with no value for field: " + fieldDescriptor.getName());
                    break;
            }
        }
        
        // Handle encrypted values if any
        if (!encryptedValues.isEmpty()) {
            String encryptedSum;
            
            if (encryptedValues.size() == 1) {
                encryptedSum = encryptedValues.get(0);
            } else {
                // homomorphic addition to sum encrypted values
                encryptedSum = encryptionService.homomorphicAdd(encryptedValues, version);
            }
            
            // decrypt final sum and add to plain sum
            plainSum += encryptionService.decryptLong(encryptedSum, version);
            plainCount += encryptedValues.size();
        }
        
        logger.fine("Sum for version " + version + ": " + plainSum + ", count: " + plainCount);
        return new VersionResult(plainSum, plainCount);
    }
} 