package nl.medtechchain.chaincode.service.query;

import com.google.protobuf.Timestamp;
import nl.medtechchain.chaincode.service.query.count.CountQuery;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceCategory;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.devicedata.MedicalSpeciality;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

// Tests for CountQuery - udi counting and distinct value counting with various edge cases
public class CountQueryTest {
    
    private TestDataGenerator generator;
    private PlatformConfig testConfig;
    
    @BeforeEach
    public void setUp() {
        generator = new TestDataGenerator(42); // fixed seed for reproducible tests
        testConfig = PlatformConfig.newBuilder().build();
    }
    
    // helper methods
    
    private Query buildCountQuery(String targetField) {
        return Query.newBuilder()
                .setQueryType(Query.QueryType.COUNT)
                .setTargetField(targetField)
                .build();
    }
    
    private long executeCount(List<DeviceDataAsset> assets, String targetField) {
        CountQuery countQuery = new CountQuery(testConfig);
        // inject test encryption service
        try {
            var encryptionServiceField = countQuery.getClass().getSuperclass().getDeclaredField("encryptionService");
            encryptionServiceField.setAccessible(true);
            encryptionServiceField.set(countQuery, new TestEncryptionService());
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject test encryption service", e);
        }
        
        Query query = buildCountQuery(targetField);
        QueryResult result = countQuery.process(query, assets);
        return result.getCountResult();
    }
    
    private long executeCountPlaintextOnly(List<DeviceDataAsset> assets, String targetField) {
        CountQuery countQuery = new CountQuery(testConfig);
        // leave encryption service as null
        
        Query query = buildCountQuery(targetField);
        QueryResult result = countQuery.process(query, assets);
        return result.getCountResult();
    }
    
    // udi field tests (simple count)
    
    @Test
    public void testUdiCountEmpty() {
        List<DeviceDataAsset> assets = Collections.emptyList();
        long result = executeCount(assets, "udi");
        
        Assertions.assertEquals(0, result, "Empty list should have count 0");
    }
    
    @Test
    public void testUdiCountSingle() {
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(new HashMap<>(), 1);
        long result = executeCount(assets, "udi");
        
        Assertions.assertEquals(1, result, "Single asset should have count 1");
    }
    
    @Test
    public void testUdiCountMultiple() {
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(new HashMap<>(), 10);
        long result = executeCount(assets, "udi");
        
        Assertions.assertEquals(10, result, "Should count all assets");
    }
    
    @Test
    public void testUdiCountLarge() {
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(new HashMap<>(), 1000);
        long result = executeCount(assets, "udi");
        
        Assertions.assertEquals(1000, result, "Should handle large asset counts");
    }
    
    // distinct value count tests - string fields
    
    @Test 
    public void testDistinctStringAllSame() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> hospitals = new HashMap<>();
        hospitals.put("Hospital A", 5);
        spec.put("hospital", hospitals);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        long result = executeCount(assets, "hospital");
        
        Assertions.assertEquals(1, result, "All same values should have count 1");
    }
    
    @Test
    public void testDistinctStringAllDifferent() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> hospitals = new HashMap<>();
        hospitals.put("Hospital A", 1);
        hospitals.put("Hospital B", 1);
        hospitals.put("Hospital C", 1);
        spec.put("hospital", hospitals);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 3);
        long result = executeCount(assets, "hospital");
        
        Assertions.assertEquals(3, result, "All different values should have count 3");
    }
    
    @Test
    public void testDistinctStringMixed() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> hospitals = new HashMap<>();
        hospitals.put("Hospital A", 3);
        hospitals.put("Hospital B", 2); 
        hospitals.put("Hospital C", 1);
        spec.put("hospital", hospitals);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 6);
        long result = executeCount(assets, "hospital");
        
        Assertions.assertEquals(3, result, "Should count distinct values not occurrences");
    }
    
    @Test
    public void testDistinctStringEmptyStrings() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> manufacturers = new HashMap<>();
        manufacturers.put("", 2); // empty strings
        manufacturers.put("Manufacturer A", 1);
        spec.put("manufacturer", manufacturers);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 3);
        long result = executeCount(assets, "manufacturer");
        
        Assertions.assertEquals(2, result, "Empty string should be counted as distinct value");
    }
    
    // distinct value count tests - integer fields
    
    @Test
    public void testDistinctIntegerValues() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> batteryLevels = new HashMap<>();
        batteryLevels.put(100, 3);
        batteryLevels.put(75, 2);
        batteryLevels.put(50, 2);
        batteryLevels.put(25, 1);
        spec.put("battery_level", batteryLevels);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 8);
        long result = executeCount(assets, "battery_level");
        
        Assertions.assertEquals(4, result, "Should count 4 distinct battery levels");
    }
    
    @Test
    public void testDistinctIntegerZeroAndNegative() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(0, 2);
        usageHours.put(-100, 1);
        usageHours.put(100, 1);
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 4);
        long result = executeCount(assets, "usage_hours");
        
        Assertions.assertEquals(3, result, "Zero and negative values should be distinct");
    }
    
    // distinct value count tests - boolean fields
    
    @Test
    public void testDistinctBooleanAllTrue() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> activeStatus = new HashMap<>();
        activeStatus.put(true, 5);
        spec.put("active_status", activeStatus);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        long result = executeCount(assets, "active_status");
        
        Assertions.assertEquals(1, result, "All true values should have count 1");
    }
    
    @Test
    public void testDistinctBooleanMixed() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> activeStatus = new HashMap<>();
        activeStatus.put(true, 3);
        activeStatus.put(false, 2);
        spec.put("active_status", activeStatus);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        long result = executeCount(assets, "active_status");
        
        Assertions.assertEquals(2, result, "Should have 2 distinct boolean values");
    }
    
    // distinct value count tests - timestamp fields
    
    @Test
    public void testDistinctTimestampValues() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> productionDates = new HashMap<>();
        Timestamp t1 = Timestamp.newBuilder().setSeconds(1000000).build();
        Timestamp t2 = Timestamp.newBuilder().setSeconds(2000000).build();
        Timestamp t3 = Timestamp.newBuilder().setSeconds(3000000).build();
        productionDates.put(t1, 2);
        productionDates.put(t2, 2);
        productionDates.put(t3, 1);
        spec.put("production_date", productionDates);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        long result = executeCount(assets, "production_date");
        
        Assertions.assertEquals(3, result, "Should count 3 distinct timestamps");
    }
    
    // distinct value count tests - enum fields
    
    @Test
    public void testDistinctDeviceCategoryValues() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> categories = new HashMap<>();
        // Test with only 1 of the 2 possible categories - this tests the distinct logic meaningfully
        categories.put(DeviceCategory.PORTABLE, 5);
        spec.put("category", categories);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 8);
        long result = executeCount(assets, "category");
        
        // Should have the 1 specified category plus potentially random ones from the remaining 3 assets
        // The generator will fill remaining assets with random values, so we could get 1 or 2 distinct values
        Assertions.assertTrue(result >= 1 && result <= 2, 
            "Should count at least the specified PORTABLE category, possibly with random WEARABLE from remaining assets");
    }
    
    @Test
    public void testDistinctMedicalSpecialityValues() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> specialities = new HashMap<>();
        specialities.put(MedicalSpeciality.CARDIOLOGY, 2);
        specialities.put(MedicalSpeciality.DIAGNOSTIC_RADIOLOGY, 2);
        specialities.put(MedicalSpeciality.NEUROLOGY, 1);
        specialities.put(MedicalSpeciality.ONCOLOGY, 1);
        spec.put("speciality", specialities);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 6);
        long result = executeCount(assets, "speciality");
        
        Assertions.assertEquals(4, result, "Should count 4 distinct specialities");
    }
    
    // null/unset field tests
    
    @Test
    public void testDistinctWithNullFields() {
        // manually create assets with some null fields
        List<DeviceDataAsset> assets = new ArrayList<>();
        
        // asset with value
        Map<String, Object> fieldValues1 = new HashMap<>();
        fieldValues1.put("model", "Model A");
        assets.add(generator.generateAsset(fieldValues1));
        
        // asset with different value
        Map<String, Object> fieldValues2 = new HashMap<>();
        fieldValues2.put("model", "Model B");
        assets.add(generator.generateAsset(fieldValues2));
        
        // assets without model field set (will be random)
        assets.add(generator.generateAsset(new HashMap<>()));
        assets.add(generator.generateAsset(new HashMap<>()));
        
        long result = executeCount(assets, "model");
        
        // should have at least 2 (Model A and Model B), possibly more from random generation
        Assertions.assertTrue(result >= 2, "Should count at least the explicitly set distinct values");
    }
    
    // encryption tests
    
    @Test
    public void testPlaintextOnlyWithNullEncryptionService() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> hospitals = new HashMap<>();
        hospitals.put("Hospital A", 2);
        hospitals.put("Hospital B", 3);
        spec.put("hospital", hospitals);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        long result = executeCountPlaintextOnly(assets, "hospital");
        
        Assertions.assertEquals(2, result, "Plaintext data should work without encryption service");
    }
    
    @Test
    public void testEncryptedDataWithNullEncryptionServiceThrowsError() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> hospitals = new HashMap<>();
        hospitals.put(TestEncryptionService.encryptString("Hospital A"), 1);
        spec.put("hospital", hospitals);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 1);
        
        IllegalStateException exception = Assertions.assertThrows(
            IllegalStateException.class,
            () -> executeCountPlaintextOnly(assets, "hospital")
        );
        
        Assertions.assertTrue(exception.getMessage().contains("Found encrypted data but no encryption service configured"));
        Assertions.assertTrue(exception.getMessage().contains("Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME"));
    }
    
    @Test
    public void testPaillierEncryptedDistinctCount() {
        CountQuery countQuery = new CountQuery(testConfig);
        
        // setup paillier encryption
        Set<String> versions = Set.of("paillier-v1");
        TestEncryptionService paillierService = new TestEncryptionService(true, false, versions, "paillier-v1");
        
        try {
            var encryptionServiceField = countQuery.getClass().getSuperclass().getDeclaredField("encryptionService");
            encryptionServiceField.setAccessible(true);
            encryptionServiceField.set(countQuery, paillierService);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject test encryption service", e);
        }
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> manufacturers = new HashMap<>();
        // paillier encrypted values
        manufacturers.put(TestEncryptionService.encryptString("Manufacturer A", "paillier-v1"), 3);
        manufacturers.put(TestEncryptionService.encryptString("Manufacturer B", "paillier-v1"), 2);
        manufacturers.put(TestEncryptionService.encryptString("Manufacturer C", "paillier-v1"), 1);
        spec.put("manufacturer", manufacturers);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 6);
        Query query = buildCountQuery("manufacturer");
        QueryResult result = countQuery.process(query, assets);
        
        Assertions.assertEquals(3, result.getCountResult(), "Should count 3 distinct encrypted values");
    }
    
    @Test
    public void testPaillierVersionGrouping() {
        CountQuery countQuery = new CountQuery(testConfig);
        
        // setup multi-version paillier
        Set<String> versions = Set.of("paillier-v1", "paillier-v2");
        TestEncryptionService paillierService = new TestEncryptionService(true, false, versions, "paillier-v2");
        
        try {
            var encryptionServiceField = countQuery.getClass().getSuperclass().getDeclaredField("encryptionService");
            encryptionServiceField.setAccessible(true);
            encryptionServiceField.set(countQuery, paillierService);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject test encryption service", e);
        }
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> models = new HashMap<>();
        // same model encrypted with different versions
        models.put(TestEncryptionService.encryptString("Model X", "paillier-v1"), 2);
        models.put(TestEncryptionService.encryptString("Model X", "paillier-v2"), 2);
        // different model
        models.put(TestEncryptionService.encryptString("Model Y", "paillier-v1"), 1);
        spec.put("model", models);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        Query query = buildCountQuery("model");
        QueryResult result = countQuery.process(query, assets);
        
        // Should count "Model X" and "Model Y" as 2 distinct values even though Model X is encrypted with 2 versions
        Assertions.assertEquals(2, result.getCountResult(), "Should count distinct decrypted values across versions");
    }
    
    // error cases
    
    @Test
    public void testUnknownFieldThrowsError() {
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(new HashMap<>(), 5);
        
        IllegalArgumentException exception = Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> executeCount(assets, "unknown_field")
        );
        
        Assertions.assertTrue(exception.getMessage().contains("Unknown target field: unknown_field"));
    }
    
    // stress tests
    
    @Test
    public void testLargeDistinctValueCount() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> models = new HashMap<>();
        
        // generate 100 distinct models
        for (int i = 0; i < 100; i++) {
            models.put("Model_" + i, 1);
        }
        spec.put("model", models);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 100);
        long result = executeCount(assets, "model");
        
        Assertions.assertEquals(100, result, "Should handle large number of distinct values");
    }
    
    @Test
    public void testLargeDatasetWithFewDistinctValues() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> categories = new HashMap<>();
        
        // Test 3 distinct categories with 1000 assets - better stress test
        categories.put(DeviceCategory.PORTABLE, 400);
        categories.put(DeviceCategory.WEARABLE, 400);
        categories.put(DeviceCategory.DEVICE_CATEGORY_UNSPECIFIED, 200);
        spec.put("category", categories);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 1000);
        long result = executeCount(assets, "category");
        
        Assertions.assertEquals(3, result, "Should efficiently count 3 distinct values in large dataset");
    }
    
    // mixed field types in same test
    
    @Test
    public void testMultipleFieldCounts() {
        // Create a dataset where all assets have specific field values
        // This tests counting different fields in the same dataset
        Map<String, Object> commonFields = new HashMap<>();
        commonFields.put("hospital", "Central Hospital");
        commonFields.put("manufacturer", "MedDevice Corp");
        commonFields.put("battery_level", 85);
        commonFields.put("active_status", true);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCoordinatedFields(commonFields, 10);
        
        // All assets have the same values, so each field should have exactly 1 distinct value
        Assertions.assertEquals(1, executeCount(assets, "hospital"), "All assets have same hospital");
        Assertions.assertEquals(1, executeCount(assets, "manufacturer"), "All assets have same manufacturer");
        Assertions.assertEquals(1, executeCount(assets, "battery_level"), "All assets have same battery level");
        Assertions.assertEquals(1, executeCount(assets, "active_status"), "All assets have same active status");
        Assertions.assertEquals(10, executeCount(assets, "udi"), "Should count all 10 assets for udi");
        
        // Now test with mixed data using the original generator behavior
        Map<String, Map<Object, Integer>> mixedSpec = new HashMap<>();
        Map<Object, Integer> hospitals = new HashMap<>();
        hospitals.put("Hospital A", 3);
        hospitals.put("Hospital B", 2);
        mixedSpec.put("hospital", hospitals);
        
        List<DeviceDataAsset> mixedAssets = generator.generateAssetsWithCounts(mixedSpec, 8);
        
        // Should have 2 distinct hospitals specified, plus potentially random ones
        long hospitalCount = executeCount(mixedAssets, "hospital");
        Assertions.assertTrue(hospitalCount >= 2, "Should have at least the 2 specified hospitals");
        Assertions.assertEquals(8, executeCount(mixedAssets, "udi"), "Should count all 8 assets for udi");
    }
} 