package nl.medtechchain.chaincode.service.query.standarddeviation;

import nl.medtechchain.chaincode.service.query.QueryProcessor;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.query.QueryResult;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.query.QueryResult.MeanAndStd;

import com.google.protobuf.Descriptors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.medtechchain.proto.devicedata.DeviceCategory;
 

public class STDQuery extends QueryProcessor {

    public STDQuery(PlatformConfig platformConfig) {
        super(platformConfig);
    }

    @Override
    public QueryResult process(Query query, List<DeviceDataAsset> assets) {
        var fieldDescriptor = getFieldDescriptor(query.getTargetField());

        double mean = calculatePopulationMean(query, assets, fieldDescriptor);
        double std = calculateSTD(query, assets, fieldDescriptor, mean);

        MeanAndStd meanAndStd = MeanAndStd.newBuilder().setMean(mean).setStd(std).build();
        return QueryResult.newBuilder().setMeanStd(meanAndStd).build();
    }



    // a helper method to calculate mean homomorphically
    private double calculatePopulationMean(Query query, List<DeviceDataAsset> assets, 
    Descriptors.FieldDescriptor fieldDescriptor) {
        if (assets.size() == 0)
            return 0;

        Map<String, List<DeviceDataAsset>> versionGroups = groupByVersion(assets);

        long sum = 0;

        for (Map.Entry<String, List<DeviceDataAsset>> entry : versionGroups.entrySet()) {
            String version = entry.getKey();
            List<DeviceDataAsset> versionAssets = entry.getValue();
            
            logger.fine("Processing mean for " + versionAssets.size() + " assets with version: " + version);
            sum += processSumVersionGroup(versionAssets, fieldDescriptor, version);
        }
        
        logger.info("Total sum across all versions: " + sum);


        return (double) sum / assets.size();
    }

    
    // a helper method to calculate std homomorphically
    private double calculateSTD(Query query, List<DeviceDataAsset> assets, 
    Descriptors.FieldDescriptor fieldDescriptor, double mean) {
        if (assets.size() == 0)
            return 0;
        Map<String, List<DeviceDataAsset>> versionGroups = groupByVersion(assets);

        double std = 0;

        for (Map.Entry<String, List<DeviceDataAsset>> entry : versionGroups.entrySet()) {
            String version = entry.getKey();
            List<DeviceDataAsset> versionAssets = entry.getValue();
            
            logger.fine("Processing std for " + versionAssets.size() + " assets with version: " + version);
            std += processSTDVersionGroup(versionAssets, fieldDescriptor, version, mean);
        }
        
        logger.info("Total std across all versions: " + std);

        double variance = std / assets.size();
        return Math.sqrt(variance);
    }


    // sums the fields of the assets with the same key version
    private long processSumVersionGroup(List<DeviceDataAsset> assets, 
                                    Descriptors.FieldDescriptor fieldDescriptor, 
                                    String version) {
        if (assets.size() == 0)
            return 0;
        long plainSum = 0;
        List<String> encryptedValues = new ArrayList<>();
        
        for (DeviceDataAsset asset : assets) {
            // Get the field value using protobuf reflection
            var fieldType = asset.getDeviceData().getField(fieldDescriptor);
            
            if (fieldType instanceof  DeviceDataAsset.IntegerField) {
                var fieldValue = (DeviceDataAsset.IntegerField) fieldType;
                            
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
            else if (fieldType instanceof DeviceDataAsset.TimestampField) {
                var fieldValue = (DeviceDataAsset.TimestampField) fieldType;

                switch (fieldValue.getFieldCase()) {
                    case PLAIN:
                        plainSum += fieldValue.getPlain().getSeconds();
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

        }
        
        // Handle encrypted values if any
        if (!encryptedValues.isEmpty()) {
            String encryptedSum;
            
            if (encryptedValues.size() == 1) {
                encryptedSum = encryptedValues.get(0);
            } else {
                // Use homomorphic addition to sum all encrypted values
                encryptedSum = encryptionService.homomorphicAdd(encryptedValues, version);
            }
            
            // Decrypt the final sum and add to plain sum
            plainSum += encryptionService.decryptLong(encryptedSum, version);
        }
        
        logger.fine("Sum for version " + version + ": " + plainSum);
        return plainSum;
    }

    private double processSTDVersionGroup(List<DeviceDataAsset> assets, 
                                    Descriptors.FieldDescriptor fieldDescriptor, 
                                    String version, double mean) {
        if (assets.size() == 0)
            return 0;
        double plainStd = 0;
        List<String> encryptedValues = new ArrayList<>();


        for (DeviceDataAsset asset : assets) {
            var fieldType = asset.getDeviceData().getField(fieldDescriptor);
            
            if (fieldType instanceof DeviceDataAsset.IntegerField) {
                var fieldValue = (DeviceDataAsset.IntegerField) fieldType;

                switch (fieldValue.getFieldCase()) {
                    case PLAIN:
                        plainStd += (fieldValue.getPlain() - mean) * (fieldValue.getPlain() - mean);
                        break;
                        
                    case ENCRYPTED:
                        if (encryptionService == null) {
                            throw new IllegalStateException("Found encrypted data but no encryption service configured. " +
                                "Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME to 'paillier' or 'bfv'.");
                        }
                        if (encryptionService.isHomomorphic()) {
                            // TODO: make BFV scheme handle it fully homomorphically
                            long decrypted = encryptionService.decryptLong(fieldValue.getEncrypted(), version);
                            plainStd += ((double) decrypted - mean) * (decrypted - mean);
                        } else {
                            // Non-homomorphic: decrypt and add to plain sum
                            long decrypted = encryptionService.decryptLong(fieldValue.getEncrypted(), version);
                            plainStd += ((double) decrypted - mean) * (decrypted - mean);
                        }
                        break;
                        
                    case FIELD_NOT_SET:
                        // Skip assets with no value for this field
                        logger.fine("Skipping asset with no value for field: " + fieldDescriptor.getName());
                        break;
                }
            } else if (fieldType instanceof DeviceDataAsset.TimestampField) {
                var fieldValue = (DeviceDataAsset.TimestampField) fieldType;

                switch (fieldValue.getFieldCase()) {
                    case PLAIN:
                        plainStd += (fieldValue.getPlain().getSeconds() - mean) * (fieldValue.getPlain().getSeconds() - mean);
                        break;
                        
                    case ENCRYPTED:
                        if (encryptionService == null) {
                            throw new IllegalStateException("Found encrypted data but no encryption service configured. " +
                                "Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME to 'paillier' or 'bfv'.");
                        }
                        if (encryptionService.isHomomorphic()) {
                            // TODO: make BFV scheme handle it fully homomorphically
                            long decrypted = encryptionService.decryptLong(fieldValue.getEncrypted(), version);
                            plainStd += ((double) decrypted - mean) * (decrypted - mean);
                        } else {
                            // Non-homomorphic: decrypt and add to plain sum
                            long decrypted = encryptionService.decryptLong(fieldValue.getEncrypted(), version);
                            plainStd += ((double) decrypted - mean) * (decrypted - mean);
                        }
                        break;
                        
                    case FIELD_NOT_SET:
                        // Skip assets with no value for this field
                        logger.fine("Skipping asset with no value for field: " + fieldDescriptor.getName());
                        break;
                }

            }
            // Get the field value using protobuf reflection
            
        }
        
        // TODO: If BFV implemented
        // mean should be encrypted with BFV
        // homomorphic subtraction and multiplication methods should be implemented
        // // no key versioning is used
        // if (!encryptedValues.isEmpty()) {
        //     String encryptedSTD;
            
        //     encryptedSTD = encryptedValues.get(0);
        //     // Use homomorphic addition to sum all encrypted values
        //     for (int i = 1; i < encryptedValues.size(); i++) {
        //         String ciphertext = encryptedValues.get(i);
        //         String subtraction = encryptionService.homomorphicSub(ciphertext, meanEncrypted);
        //         String squared = encryptionService.homomorphicMultiply(subtraction, subtraction);
        //         encryptedSTD += encryptionService.homomomorphicAdd(squared, encryptedSTD);
        //     }            
        //     // Decrypt the final sum and add to plain sum
        //     plainStd += encryptionService.decryptLong(encryptedSum);
        // }

        logger.fine("Std for version " + version + ": " + plainStd);

        return plainStd;
    }
}