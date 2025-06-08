package nl.medtechchain.chaincode.service.query.groupedcount;

import com.google.protobuf.Descriptors;
import nl.medtechchain.chaincode.service.query.QueryProcessor;
import nl.medtechchain.chaincode.service.solver.ILPSolver;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceCategory;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.devicedata.MedicalSpeciality;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;

import java.util.*;

// Count occurrences per distinct value - uses ILP solver for encrypted categorical data
public class GroupedCountQuery extends QueryProcessor {
    
    private final ILPSolver ilpSolver = new ILPSolver();
    
    public GroupedCountQuery(PlatformConfig platformConfig) {
        super(platformConfig);
    }
    
    @Override
    public QueryResult process(Query query, List<DeviceDataAsset> assets) {
        var fieldDescriptor = getFieldDescriptor(query.getTargetField());
        if (fieldDescriptor == null) {
            throw new IllegalArgumentException("Unknown target field: " + query.getTargetField());
        }
        
        Map<String, Long> groupedCounts = new HashMap<>();
        
        // Group by version for optimal processing
        Map<String, List<DeviceDataAsset>> versionGroups = groupByVersion(assets);
        
        for (Map.Entry<String, List<DeviceDataAsset>> entry : versionGroups.entrySet()) {
            String version = entry.getKey();
            List<DeviceDataAsset> versionAssets = entry.getValue();
            
            logger.fine("Processing grouped count for " + versionAssets.size() + " assets with version: " + version);
            
            processVersionGroup(versionAssets, fieldDescriptor, version, groupedCounts);
        }
        
        // Remove zero counts
        groupedCounts.entrySet().removeIf(e -> e.getValue() == 0);
        
        logger.info("Grouped count complete. Found " + groupedCounts.size() + " distinct values");
        
        return QueryResult.newBuilder()
            .setGroupedCountResult(QueryResult.GroupedCount.newBuilder()
                .putAllMap(groupedCounts)
                .build())
            .build();
    }
    
    private void processVersionGroup(List<DeviceDataAsset> versionAssets,
                                   Descriptors.FieldDescriptor fieldDescriptor,
                                   String version,
                                   Map<String, Long> groupedCounts) {
        
        // Determine field type
        Object sampleField = versionAssets.isEmpty() ? null : 
            versionAssets.get(0).getDeviceData().getField(fieldDescriptor);
            
        if (sampleField instanceof DeviceDataAsset.BoolField) {
            processBooleanGroup(versionAssets, fieldDescriptor, version, groupedCounts);
        } else if (sampleField instanceof DeviceDataAsset.DeviceCategoryField) {
            processDeviceCategoryGroup(versionAssets, fieldDescriptor, version, groupedCounts);
        } else if (sampleField instanceof DeviceDataAsset.MedicalSpecialityField) {
            processMedicalSpecialityGroup(versionAssets, fieldDescriptor, version, groupedCounts);
        } else {
            // For non-categorical fields, process each individually
            for (DeviceDataAsset asset : versionAssets) {
                Object fieldValue = asset.getDeviceData().getField(fieldDescriptor);
                String value = extractFieldValueAsString(fieldValue, version);
                if (value != null) {
                    groupedCounts.merge(value, 1L, Long::sum);
                }
            }
        }
    }
    
    private void processBooleanGroup(List<DeviceDataAsset> assets,
                                   Descriptors.FieldDescriptor fieldDescriptor,
                                   String version,
                                   Map<String, Long> groupedCounts) {
        List<String> encryptedValues = new ArrayList<>();
        
        // Process all assets, collecting encrypted values for homomorphic processing
        for (DeviceDataAsset asset : assets) {
            DeviceDataAsset.BoolField field = (DeviceDataAsset.BoolField) asset.getDeviceData().getField(fieldDescriptor);
            
            switch (field.getFieldCase()) {
                case PLAIN:
                    String key = String.valueOf(field.getPlain());
                    groupedCounts.merge(key, 1L, Long::sum);
                    break;
                    
                case ENCRYPTED:
                    if (encryptionService == null) {
                        throw new IllegalStateException("Found encrypted data but no encryption service configured. " +
                            "Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME to 'paillier' or 'bfv'.");
                    }
                    if (encryptionService.isHomomorphic()) {
                        // Collect for batch homomorphic processing
                        encryptedValues.add(field.getEncrypted());
                    } else {
                        // Non-homomorphic: decrypt immediately
                        boolean value = encryptionService.decryptBool(field.getEncrypted(), version);
                        groupedCounts.merge(String.valueOf(value), 1L, Long::sum);
                    }
                    break;
            }
        }
        
        // Process all collected encrypted values homomorphically
        if (!encryptedValues.isEmpty()) {
            processHomomorphicBooleans(encryptedValues, version, groupedCounts);
        }
    }
    
    private void processDeviceCategoryGroup(List<DeviceDataAsset> assets,
                                          Descriptors.FieldDescriptor fieldDescriptor,
                                          String version,
                                          Map<String, Long> groupedCounts) {
        List<String> encryptedValues = new ArrayList<>();
        
        for (DeviceDataAsset asset : assets) {
            DeviceDataAsset.DeviceCategoryField field = 
                (DeviceDataAsset.DeviceCategoryField) asset.getDeviceData().getField(fieldDescriptor);
            
            switch (field.getFieldCase()) {
                case PLAIN:
                    String key = field.getPlain().name();
                    groupedCounts.merge(key, 1L, Long::sum);
                    break;
                    
                case ENCRYPTED:
                    if (encryptionService == null) {
                        throw new IllegalStateException("Found encrypted data but no encryption service configured. " +
                            "Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME to 'paillier' or 'bfv'.");
                    }
                    if (encryptionService.isHomomorphic()) {
                        encryptedValues.add(field.getEncrypted());
                    } else {
                        long decrypted = encryptionService.decryptLong(field.getEncrypted(), version);
                        DeviceCategory category = DeviceCategory.forNumber((int) decrypted);
                        groupedCounts.merge(category.name(), 1L, Long::sum);
                    }
                    break;
            }
        }
        
        if (!encryptedValues.isEmpty()) {
            processHomomorphicDeviceCategories(encryptedValues, version, groupedCounts);
        }
    }
    
    private void processMedicalSpecialityGroup(List<DeviceDataAsset> assets,
                                             Descriptors.FieldDescriptor fieldDescriptor,
                                             String version,
                                             Map<String, Long> groupedCounts) {
        List<String> encryptedValues = new ArrayList<>();
        
        for (DeviceDataAsset asset : assets) {
            DeviceDataAsset.MedicalSpecialityField field = 
                (DeviceDataAsset.MedicalSpecialityField) asset.getDeviceData().getField(fieldDescriptor);
            
            switch (field.getFieldCase()) {
                case PLAIN:
                    String key = field.getPlain().name();
                    groupedCounts.merge(key, 1L, Long::sum);
                    break;
                    
                case ENCRYPTED:
                    if (encryptionService == null) {
                        throw new IllegalStateException("Found encrypted data but no encryption service configured. " +
                            "Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME to 'paillier' or 'bfv'.");
                    }
                    if (encryptionService.isHomomorphic()) {
                        encryptedValues.add(field.getEncrypted());
                    } else {
                        long decrypted = encryptionService.decryptLong(field.getEncrypted(), version);
                        MedicalSpeciality speciality = MedicalSpeciality.forNumber((int) decrypted);
                        groupedCounts.merge(speciality.name(), 1L, Long::sum);
                    }
                    break;
            }
        }
        
        if (!encryptedValues.isEmpty()) {
            processHomomorphicMedicalSpecialities(encryptedValues, version, groupedCounts);
        }
    }
    
    private void processHomomorphicBooleans(List<String> encryptedValues, String version,
                                          Map<String, Long> groupedCounts) {
        if (encryptedValues.isEmpty()) return;
        
        // Homomorphically add all boolean values
        String homomorphicSum = encryptionService.homomorphicAdd(encryptedValues, version);
        long decryptedSum = encryptionService.decryptLong(homomorphicSum, version);
        
        // Use ILP solver to reconstruct counts
        var solution = ilpSolver.solveSystem(
            List.of("false", "true"),
            List.of(0, 1),
            decryptedSum,
            encryptedValues.size()
        );
        
        if (solution.isPresent()) {
            for (Map.Entry<String, Integer> entry : solution.get().entrySet()) {
                groupedCounts.merge(entry.getKey(), entry.getValue().longValue(), Long::sum);
            }
        } else {
            logger.warning("ILP solver failed for boolean field. Falling back to individual decryption.");
            // Fallback: decrypt each value
            for (String encrypted : encryptedValues) {
                boolean value = encryptionService.decryptBool(encrypted, version);
                groupedCounts.merge(String.valueOf(value), 1L, Long::sum);
            }
        }
    }
    
    private void processHomomorphicDeviceCategories(List<String> encryptedValues, String version,
                                                  Map<String, Long> groupedCounts) {
        if (encryptedValues.isEmpty()) return;
        
        String homomorphicSum = encryptionService.homomorphicAdd(encryptedValues, version);
        long decryptedSum = encryptionService.decryptLong(homomorphicSum, version);
        
        // Get valid enum values
        List<String> names = new ArrayList<>();
        List<Integer> numbers = new ArrayList<>();
        for (DeviceCategory cat : DeviceCategory.values()) {
            if (cat != DeviceCategory.UNRECOGNIZED && cat != DeviceCategory.DEVICE_CATEGORY_UNSPECIFIED) {
                names.add(cat.name());
                numbers.add(cat.getNumber());
            }
        }
        
        var solution = ilpSolver.solveSystem(names, numbers, decryptedSum, encryptedValues.size());
        
        if (solution.isPresent()) {
            for (Map.Entry<String, Integer> entry : solution.get().entrySet()) {
                groupedCounts.merge(entry.getKey(), entry.getValue().longValue(), Long::sum);
            }
        } else {
            logger.warning("ILP solver failed for device category. Falling back to individual decryption.");
            for (String encrypted : encryptedValues) {
                long decrypted = encryptionService.decryptLong(encrypted, version);
                DeviceCategory category = DeviceCategory.forNumber((int) decrypted);
                groupedCounts.merge(category.name(), 1L, Long::sum);
            }
        }
    }
    
    private void processHomomorphicMedicalSpecialities(List<String> encryptedValues, String version,
                                                     Map<String, Long> groupedCounts) {
        if (encryptedValues.isEmpty()) return;
        
        String homomorphicSum = encryptionService.homomorphicAdd(encryptedValues, version);
        long decryptedSum = encryptionService.decryptLong(homomorphicSum, version);
        
        List<String> names = new ArrayList<>();
        List<Integer> numbers = new ArrayList<>();
        for (MedicalSpeciality spec : MedicalSpeciality.values()) {
            if (spec != MedicalSpeciality.UNRECOGNIZED && spec != MedicalSpeciality.MEDICAL_SPECIALITY_UNSPECIFIED) {
                names.add(spec.name());
                numbers.add(spec.getNumber());
            }
        }
        
        var solution = ilpSolver.solveSystem(names, numbers, decryptedSum, encryptedValues.size());
        
        if (solution.isPresent()) {
            for (Map.Entry<String, Integer> entry : solution.get().entrySet()) {
                groupedCounts.merge(entry.getKey(), entry.getValue().longValue(), Long::sum);
            }
        } else {
            logger.warning("ILP solver failed for medical speciality. Falling back to individual decryption.");
            for (String encrypted : encryptedValues) {
                long decrypted = encryptionService.decryptLong(encrypted, version);
                MedicalSpeciality speciality = MedicalSpeciality.forNumber((int) decrypted);
                groupedCounts.merge(speciality.name(), 1L, Long::sum);
            }
        }
    }
    
    private String extractFieldValueAsString(Object fieldValue, String version) {
        // For non-categorical fields that need immediate decryption
        if (fieldValue instanceof DeviceDataAsset.StringField) {
            DeviceDataAsset.StringField field = (DeviceDataAsset.StringField) fieldValue;
            switch (field.getFieldCase()) {
                case PLAIN: return field.getPlain();
                case ENCRYPTED: 
                    if (encryptionService == null) {
                        throw new IllegalStateException("Found encrypted data but no encryption service configured. " +
                            "Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME to 'paillier' or 'bfv'.");
                    }
                    return encryptionService.decryptString(field.getEncrypted(), version);
            }
        } else if (fieldValue instanceof DeviceDataAsset.IntegerField) {
            DeviceDataAsset.IntegerField field = (DeviceDataAsset.IntegerField) fieldValue;
            switch (field.getFieldCase()) {
                case PLAIN: return String.valueOf(field.getPlain());
                case ENCRYPTED: 
                    if (encryptionService == null) {
                        throw new IllegalStateException("Found encrypted data but no encryption service configured. " +
                            "Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME to 'paillier' or 'bfv'.");
                    }
                    return String.valueOf(encryptionService.decryptLong(field.getEncrypted(), version));
            }
        } else if (fieldValue instanceof DeviceDataAsset.TimestampField) {
            DeviceDataAsset.TimestampField field = (DeviceDataAsset.TimestampField) fieldValue;
            switch (field.getFieldCase()) {
                case PLAIN: return String.valueOf(field.getPlain().getSeconds());
                case ENCRYPTED: 
                    if (encryptionService == null) {
                        throw new IllegalStateException("Found encrypted data but no encryption service configured. " +
                            "Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME to 'paillier' or 'bfv'.");
                    }
                    return String.valueOf(encryptionService.decryptLong(field.getEncrypted(), version));
            }
        }
        return null;
    }
} 