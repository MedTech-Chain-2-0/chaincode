package nl.medtechchain.chaincode.service.query.sum;

import com.google.protobuf.Descriptors;

import nl.medtechchain.chaincode.service.query.QueryProcessor;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.medtechchain.chaincode.service.encryption.BfvEncryptionService;
import nl.medtechchain.chaincode.service.encryption.PaillierEncryptionService;

// Sums up integer fields - handles mixed plain/encrypted data
public class SumQuery extends QueryProcessor {
    
    public SumQuery(PlatformConfig platformConfig) {
        super(platformConfig);
    }
    
    @Override
    public QueryResult process(Query query, List<DeviceDataAsset> assets) {
        var fieldDescriptor = getFieldDescriptor(query.getTargetField());
        
        // Group by version for optimal homomorphic operations
        Map<String, List<DeviceDataAsset>> versionGroups = groupByVersion(assets);
        
        long totalSum = 0;
        
        for (Map.Entry<String, List<DeviceDataAsset>> entry : versionGroups.entrySet()) {
            String version = entry.getKey();
            List<DeviceDataAsset> versionAssets = entry.getValue();
            
            logger.fine("Processing sum for " + versionAssets.size() + " assets with version: " + version);
            
            totalSum += processVersionGroup(versionAssets, fieldDescriptor, version);
        }
        
        logger.info("Total sum across all versions: " + totalSum);
        return QueryResult.newBuilder().setSumResult(totalSum).build();
    }
    
    private long processVersionGroup(List<DeviceDataAsset> assets, 
                                    Descriptors.FieldDescriptor fieldDescriptor, 
                                    String version) {
        long plainSum = 0;
        List<String> encryptedValues = new ArrayList<>();
        
        for (DeviceDataAsset asset : assets) {
            // Get the field value using protobuf reflection
            var fieldValue = (DeviceDataAsset.IntegerField) asset.getDeviceData().getField(fieldDescriptor);
            
            switch (fieldValue.getFieldCase()) {
                case PLAIN:
                    plainSum += fieldValue.getPlain();
                    break;
                    
                case ENCRYPTED:
                    if (encryptionService == null) {
                        throw new IllegalStateException("Found encrypted data but no encryption service configured. " +
                            "Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME to 'paillier' or 'bfv'.");
                    }
                    if (encryptionService.isHomomorphic()) {
                        // Collect encrypted values for homomorphic addition
                        encryptedValues.add(fieldValue.getEncrypted());
                    } else {
                        // Non-homomorphic: decrypt and add to plain sum
                        plainSum += encryptionService.decryptLong(fieldValue.getEncrypted(), version);
                    }
                    break;
                    
                case FIELD_NOT_SET:
                    // Skip assets with no value for this field
                    logger.fine("Skipping asset with no value for field: " + fieldDescriptor.getName());
                    break;
            }
        }
        
        // Handle encrypted values if any
        if (!encryptedValues.isEmpty()) {
            String encryptedSum;
            
            if (encryptedValues.size() == 1) {
                encryptedSum = encryptedValues.get(0);
            } else if (encryptionService instanceof  PaillierEncryptionService) {
                // Use homomorphic addition to sum all encrypted values
                encryptedSum = encryptionService.homomorphicAdd(encryptedValues, version);
            }
            else if (encryptionService instanceof BfvEncryptionService) {
                encryptedSum = encryptionService.homomorphicAdd(encryptedValues, null);
            }
            else {
                encryptedSum = "";
            }
            
            // Decrypt the final sum and add to plain sum
            plainSum += encryptionService.decryptLong(encryptedSum, version);
        }
        
        logger.fine("Sum for version " + version + ": " + plainSum);
        return plainSum;
    }
} 