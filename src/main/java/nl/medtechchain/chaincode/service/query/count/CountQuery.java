package nl.medtechchain.chaincode.service.query.count;

import com.google.protobuf.Descriptors;
import nl.medtechchain.chaincode.service.query.QueryProcessor;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

// Counts distinct values or just asset count (for "udi" field)
public class CountQuery extends QueryProcessor {
    
    public CountQuery(PlatformConfig platformConfig) {
        super(platformConfig);
    }
    
    @Override
    public QueryResult process(Query query, List<DeviceDataAsset> assets) {
        int count;
        
        if ("udi".equals(query.getTargetField())) {
            // Simple count - just return the number of assets
            count = assets.size();
            logger.info("Simple count (udi): " + count);
        } else {
            // Count distinct values in the target field
            count = countDistinctValues(query, assets);
            logger.info("Distinct value count for field " + query.getTargetField() + ": " + count);
        }
        
        return QueryResult.newBuilder().setCountResult(count).build();
    }
    
    private int countDistinctValues(Query query, List<DeviceDataAsset> assets) {
        var fieldDescriptor = getFieldDescriptor(query.getTargetField());
        if (fieldDescriptor == null) {
            throw new IllegalArgumentException("Unknown target field: " + query.getTargetField());
        }
        
        // Group by version for optimal processing
        Map<String, List<DeviceDataAsset>> versionGroups = groupByVersion(assets);
        
        // Collect all unique values across all versions
        Set<String> uniqueValues = new HashSet<>();
        
        for (Map.Entry<String, List<DeviceDataAsset>> entry : versionGroups.entrySet()) {
            String version = entry.getKey();
            List<DeviceDataAsset> versionAssets = entry.getValue();
            
            logger.fine("Processing " + versionAssets.size() + " assets with version: " + version);
            
            processVersionGroup(versionAssets, fieldDescriptor, version, uniqueValues);
        }
        
        return uniqueValues.size();
    }
    
    private void processVersionGroup(List<DeviceDataAsset> assets, 
                                   Descriptors.FieldDescriptor fieldDescriptor,
                                   String version,
                                   Set<String> uniqueValues) {
        
        // For Paillier, we cannot compare encrypted values directly
        // We must decrypt to identify unique values
        
        for (DeviceDataAsset asset : assets) {
            String value = extractFieldValue(asset, fieldDescriptor, version);
            if (value != null) {
                uniqueValues.add(value);
            }
        }
    }
    
    private String extractFieldValue(DeviceDataAsset asset, 
                                   Descriptors.FieldDescriptor fieldDescriptor,
                                   String version) {
        Object fieldValue = asset.getDeviceData().getField(fieldDescriptor);
        
        // Handle different field types
        if (fieldValue instanceof DeviceDataAsset.StringField) {
            return extractStringValue((DeviceDataAsset.StringField) fieldValue, version);
        } else if (fieldValue instanceof DeviceDataAsset.IntegerField) {
            return extractIntegerValue((DeviceDataAsset.IntegerField) fieldValue, version);
        } else if (fieldValue instanceof DeviceDataAsset.BoolField) {
            return extractBoolValue((DeviceDataAsset.BoolField) fieldValue, version);
        } else if (fieldValue instanceof DeviceDataAsset.TimestampField) {
            return extractTimestampValue((DeviceDataAsset.TimestampField) fieldValue, version);
        } else if (fieldValue instanceof DeviceDataAsset.DeviceCategoryField) {
            return extractDeviceCategoryValue((DeviceDataAsset.DeviceCategoryField) fieldValue, version);
        } else if (fieldValue instanceof DeviceDataAsset.MedicalSpecialityField) {
            return extractMedicalSpecialityValue((DeviceDataAsset.MedicalSpecialityField) fieldValue, version);
        }
        
        return null;
    }
    
    private String extractStringValue(DeviceDataAsset.StringField field, String version) {
        switch (field.getFieldCase()) {
            case PLAIN:
                return field.getPlain();
            case ENCRYPTED:
                if (encryptionService == null) {
                    throw new IllegalStateException("Found encrypted data but no encryption service configured. " +
                        "Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME to 'paillier' or 'bfv'.");
                }
                return encryptionService.decryptString(field.getEncrypted(), version);
            case FIELD_NOT_SET:
                return null;
        }
        return null;
    }
    
    private String extractIntegerValue(DeviceDataAsset.IntegerField field, String version) {
        switch (field.getFieldCase()) {
            case PLAIN:
                return String.valueOf(field.getPlain());
            case ENCRYPTED:
                if (encryptionService == null) {
                    throw new IllegalStateException("Found encrypted data but no encryption service configured. " +
                        "Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME to 'paillier' or 'bfv'.");
                }
                return String.valueOf(encryptionService.decryptLong(field.getEncrypted(), version));
            case FIELD_NOT_SET:
                return null;
        }
        return null;
    }
    
    private String extractBoolValue(DeviceDataAsset.BoolField field, String version) {
        switch (field.getFieldCase()) {
            case PLAIN:
                return String.valueOf(field.getPlain());
            case ENCRYPTED:
                if (encryptionService == null) {
                    throw new IllegalStateException("Found encrypted data but no encryption service configured. " +
                        "Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME to 'paillier' or 'bfv'.");
                }
                return String.valueOf(encryptionService.decryptBool(field.getEncrypted(), version));
            case FIELD_NOT_SET:
                return null;
        }
        return null;
    }
    
    private String extractTimestampValue(DeviceDataAsset.TimestampField field, String version) {
        switch (field.getFieldCase()) {
            case PLAIN:
                return String.valueOf(field.getPlain().getSeconds());
            case ENCRYPTED:
                if (encryptionService == null) {
                    throw new IllegalStateException("Found encrypted data but no encryption service configured. " +
                        "Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME to 'paillier' or 'bfv'.");
                }
                return String.valueOf(encryptionService.decryptLong(field.getEncrypted(), version));
            case FIELD_NOT_SET:
                return null;
        }
        return null;
    }
    
    private String extractDeviceCategoryValue(DeviceDataAsset.DeviceCategoryField field, String version) {
        switch (field.getFieldCase()) {
            case PLAIN:
                return field.getPlain().name();
            case ENCRYPTED:
                if (encryptionService == null) {
                    throw new IllegalStateException("Found encrypted data but no encryption service configured. " +
                        "Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME to 'paillier' or 'bfv'.");
                }
                long decrypted = encryptionService.decryptLong(field.getEncrypted(), version);
                return nl.medtechchain.proto.devicedata.DeviceCategory.forNumber((int) decrypted).name();
            case FIELD_NOT_SET:
                return null;
        }
        return null;
    }
    
    private String extractMedicalSpecialityValue(DeviceDataAsset.MedicalSpecialityField field, String version) {
        switch (field.getFieldCase()) {
            case PLAIN:
                return field.getPlain().name();
            case ENCRYPTED:
                if (encryptionService == null) {
                    throw new IllegalStateException("Found encrypted data but no encryption service configured. " +
                        "Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME to 'paillier' or 'bfv'.");
                }
                long decrypted = encryptionService.decryptLong(field.getEncrypted(), version);
                return nl.medtechchain.proto.devicedata.MedicalSpeciality.forNumber((int) decrypted).name();
            case FIELD_NOT_SET:
                return null;
        }
        return null;
    }
} 