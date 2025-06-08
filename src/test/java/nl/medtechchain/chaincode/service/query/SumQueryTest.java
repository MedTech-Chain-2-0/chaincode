package nl.medtechchain.chaincode.service.query;

import nl.medtechchain.chaincode.service.query.sum.SumQuery;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

// Tests for SumQuery - covers plaintext only, paillier only, and error cases
public class SumQueryTest {
    
    private TestDataGenerator generator;
    private PlatformConfig testConfig;
    
    @BeforeEach
    public void setUp() {
        generator = new TestDataGenerator(42); // Fixed seed for reproducible tests
        testConfig = PlatformConfig.newBuilder().build();
    }
    
    // helper methods
    
    private Query buildSumQuery(String targetField) {
        return Query.newBuilder()
                .setQueryType(Query.QueryType.SUM)
                .setTargetField(targetField)
                .build();
    }
    
    private long executeSum(List<DeviceDataAsset> assets, String targetField) {
        SumQuery sumQuery = new SumQuery(testConfig);
        // inject test encryption service via reflection
        try {
            var encryptionServiceField = sumQuery.getClass().getSuperclass().getDeclaredField("encryptionService");
            encryptionServiceField.setAccessible(true);
            encryptionServiceField.set(sumQuery, new TestEncryptionService());
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject test encryption service", e);
        }
        
        Query query = buildSumQuery(targetField);
        QueryResult result = sumQuery.process(query, assets);
        return result.getSumResult();
    }
    
    // helper for plaintext-only tests (null encryption service)
    private long executeSumPlaintextOnly(List<DeviceDataAsset> assets, String targetField) {
        SumQuery sumQuery = new SumQuery(testConfig);
        // leave encryption service as null
        
        Query query = buildSumQuery(targetField);
        QueryResult result = sumQuery.process(query, assets);
        return result.getSumResult();
    }
    
    // basic functionality tests
    
    @Test
    public void testSingleAsset() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(100, 1);
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 1);
        long result = executeSum(assets, "usage_hours");
        
        Assertions.assertEquals(100, result, "Single asset sum should be 100");
    }
    
    @Test
    public void testMultipleAssetsSameValue() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(50, 5); // 5 assets with value 50
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        long result = executeSum(assets, "usage_hours");
        
        Assertions.assertEquals(250, result, "Sum of 5 assets with value 50 should be 250");
    }
    
    @Test
    public void testMultipleAssetsDifferentValues() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(100, 3); // 3 assets with value 100
        usageHours.put(200, 2); // 2 assets with value 200
        usageHours.put(300, 1); // 1 asset with value 300
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 6);
        long result = executeSum(assets, "usage_hours");
        
        long expected = 100 * 3 + 200 * 2 + 300 * 1; // = 300 + 400 + 300 = 1000
        Assertions.assertEquals(expected, result, "Sum should be 1000");
    }
    
    @Test
    public void testEmptyAssetList() {
        List<DeviceDataAsset> assets = Collections.emptyList();
        long result = executeSum(assets, "usage_hours");
        
        Assertions.assertEquals(0, result, "Sum of empty list should be 0");
    }
    
    @Test
    public void testZeroValues() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(0, 3); // 3 assets with value 0
        usageHours.put(100, 2); // 2 assets with value 100
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        long result = executeSum(assets, "usage_hours");
        
        Assertions.assertEquals(200, result, "Sum should ignore zeros: 0*3 + 100*2 = 200");
    }
    
    // edge cases and error handling
    
    @Test
    public void testLargeValues() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(1000000, 2); // 2 assets with value 1,000,000
        usageHours.put(2000000, 1); // 1 asset with value 2,000,000
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 3);
        long result = executeSum(assets, "usage_hours");
        
        long expected = 1000000L * 2 + 2000000L; // = 4,000,000
        Assertions.assertEquals(expected, result, "Large values should be handled correctly");
    }
    
    @Test
    public void testNegativeValues() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(-100, 2); // 2 assets with value -100
        usageHours.put(300, 1); // 1 asset with value 300
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 3);
        long result = executeSum(assets, "usage_hours");
        
        long expected = -100 * 2 + 300; // = -200 + 300 = 100
        Assertions.assertEquals(expected, result, "Negative values should be handled correctly");
    }
    
    @Test
    public void testPlaintextOnlyWithNullEncryptionService() {
        // test pure plaintext data with no encryption service configured
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(100, 2);
        usageHours.put(200, 3);
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        long result = executeSumPlaintextOnly(assets, "usage_hours");
        
        long expected = 100 * 2 + 200 * 3; // = 200 + 600 = 800
        Assertions.assertEquals(expected, result, "Plaintext data should work fine without encryption service");
    }
    
    @Test
    public void testEncryptedDataWithNullEncryptionServiceThrowsError() {
        // test that encrypted data without encryption service gives clear error
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(TestEncryptionService.encryptLong(100), 1);
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 1);
        
        IllegalStateException exception = Assertions.assertThrows(
            IllegalStateException.class,
            () -> executeSumPlaintextOnly(assets, "usage_hours")
        );
        
        Assertions.assertTrue(exception.getMessage().contains("Found encrypted data but no encryption service configured"));
        Assertions.assertTrue(exception.getMessage().contains("Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME"));
    }
    
    // encryption tests
    
    @Test
    public void testPaillierEncryptedHomomorphic() {
        SumQuery sumQuery = new SumQuery(testConfig);
        
        // setup paillier encryption
        Set<String> versions = Set.of("paillier-v1");
        TestEncryptionService paillierService = new TestEncryptionService(true, false, versions, "paillier-v1");
        
        try {
            var encryptionServiceField = sumQuery.getClass().getSuperclass().getDeclaredField("encryptionService");
            encryptionServiceField.setAccessible(true);
            encryptionServiceField.set(sumQuery, paillierService);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject test encryption service", e);
        }
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        
        // only paillier encrypted values
        usageHours.put(TestEncryptionService.encryptLong(150, "paillier-v1"), 2);
        usageHours.put(TestEncryptionService.encryptLong(250, "paillier-v1"), 3);
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        Query query = buildSumQuery("usage_hours");
        QueryResult result = sumQuery.process(query, assets);
        
        long expected = 150 * 2 + 250 * 3; // = 300 + 750 = 1050
        Assertions.assertEquals(expected, result.getSumResult(), "Paillier encrypted values should be processed correctly");
        
        // verify all assets are paillier encrypted
        for (DeviceDataAsset asset : assets) {
            Assertions.assertEquals("paillier-v1", asset.getKeyVersion());
            Assertions.assertEquals(DeviceDataAsset.IntegerField.FieldCase.ENCRYPTED, 
                    asset.getDeviceData().getUsageHours().getFieldCase());
        }
    }
    
    // ================ Pure Plaintext Tests ================
    
    @Test
    public void testPurePlaintextData() {
        // Test with only plaintext data (encryption config = "none")
        SumQuery sumQuery = new SumQuery(testConfig);
        
        // Configure for plaintext-only (non-homomorphic to simulate "none" config)
        TestEncryptionService plaintextService = new TestEncryptionService(false, false);
        
        try {
            var encryptionServiceField = sumQuery.getClass().getSuperclass().getDeclaredField("encryptionService");
            encryptionServiceField.setAccessible(true);
            encryptionServiceField.set(sumQuery, plaintextService);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject test encryption service", e);
        }
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        
        // Only plaintext values
        usageHours.put(100, 3);
        usageHours.put(200, 2);
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        Query query = buildSumQuery("usage_hours");
        QueryResult result = sumQuery.process(query, assets);
        
        long expected = 100 * 3 + 200 * 2; // = 500
        Assertions.assertEquals(expected, result.getSumResult(), "Pure plaintext data should work correctly");
        
        // Verify all assets are plaintext
        for (DeviceDataAsset asset : assets) {
            Assertions.assertEquals("plaintext", asset.getKeyVersion());
            Assertions.assertEquals(DeviceDataAsset.IntegerField.FieldCase.PLAIN, 
                    asset.getDeviceData().getUsageHours().getFieldCase());
        }
    }
    
    @Test
    public void testPaillierEncryptedLargeDataset() {
        // Test Paillier encryption with larger dataset to verify homomorphic efficiency
        SumQuery sumQuery = new SumQuery(testConfig);
        
        // Configure for Paillier encryption
        Set<String> versions = Set.of("paillier-v1");
        TestEncryptionService paillierService = new TestEncryptionService(true, false, versions, "paillier-v1");
        
        try {
            var encryptionServiceField = sumQuery.getClass().getSuperclass().getDeclaredField("encryptionService");
            encryptionServiceField.setAccessible(true);
            encryptionServiceField.set(sumQuery, paillierService);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject test encryption service", e);
        }
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        
        // Larger dataset with Paillier encryption
        usageHours.put(TestEncryptionService.encryptLong(100, "paillier-v1"), 10);
        usageHours.put(TestEncryptionService.encryptLong(200, "paillier-v1"), 5);
        usageHours.put(TestEncryptionService.encryptLong(300, "paillier-v1"), 3);
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 18);
        Query query = buildSumQuery("usage_hours");
        QueryResult result = sumQuery.process(query, assets);
        
        long expected = 100 * 10 + 200 * 5 + 300 * 3; // = 1000 + 1000 + 900 = 2900
        Assertions.assertEquals(expected, result.getSumResult(), 
                "Paillier encryption should handle larger datasets efficiently");
    }
    
    // ================ Different Field Tests ================
    
    @Test
    public void testBatteryLevelField() {
        // Test summing a different integer field
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> batteryLevel = new HashMap<>();
        batteryLevel.put(75, 4); // 4 devices at 75% battery
        batteryLevel.put(50, 2); // 2 devices at 50% battery
        spec.put("battery_level", batteryLevel);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 6);
        long result = executeSum(assets, "battery_level");
        
        long expected = 75 * 4 + 50 * 2; // = 300 + 100 = 400
        Assertions.assertEquals(expected, result, "Battery level sum should be 400");
    }
    
    @Test
    public void testSyncFrequencyField() {
        // Test summing sync frequency
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> syncFreq = new HashMap<>();
        syncFreq.put(60, 5); // 5 devices with 60-second sync
        syncFreq.put(120, 3); // 3 devices with 120-second sync
        spec.put("sync_frequency_seconds", syncFreq);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 8);
        long result = executeSum(assets, "sync_frequency_seconds");
        
        long expected = 60 * 5 + 120 * 3; // = 300 + 360 = 660
        Assertions.assertEquals(expected, result, "Sync frequency sum should be 660");
    }
    
    // ================ Paillier Version Tests ================
    
    @Test
    public void testPaillierVersionGrouping() {
        // Test version grouping within Paillier scheme (e.g., v1 and v2 keys)
        SumQuery sumQuery = new SumQuery(testConfig);
        
        // Configure for multiple Paillier versions
        Set<String> versions = Set.of("paillier-v1", "paillier-v2");
        TestEncryptionService multiVersionService = new TestEncryptionService(true, false, versions, "paillier-v2");
        
        try {
            var encryptionServiceField = sumQuery.getClass().getSuperclass().getDeclaredField("encryptionService");
            encryptionServiceField.setAccessible(true);
            encryptionServiceField.set(sumQuery, multiVersionService);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject test encryption service", e);
        }
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        
        // Assets encrypted with different Paillier versions
        usageHours.put(TestEncryptionService.encryptLong(100, "paillier-v1"), 3);  // Old version
        usageHours.put(TestEncryptionService.encryptLong(200, "paillier-v2"), 2);  // New version
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        Query query = buildSumQuery("usage_hours");
        QueryResult result = sumQuery.process(query, assets);
        
        long expected = 100 * 3 + 200 * 2; // = 300 + 400 = 700
        Assertions.assertEquals(expected, result.getSumResult(), 
                "Should correctly handle multiple Paillier versions: " + expected);
        
        // Verify we have both Paillier versions
        Set<String> actualVersions = new HashSet<>();
        for (DeviceDataAsset asset : assets) {
            actualVersions.add(asset.getKeyVersion());
        }
        Assertions.assertTrue(actualVersions.contains("paillier-v1"), "Should have paillier-v1 assets");
        Assertions.assertTrue(actualVersions.contains("paillier-v2"), "Should have paillier-v2 assets");
    }
    

    
    // ================ Stress Tests ================
    
    @Test
    public void testManyAssets() {
        // Test with a larger number of assets
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(10, 100); // 100 assets with value 10
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 100);
        long result = executeSum(assets, "usage_hours");
        
        Assertions.assertEquals(1000, result, "Sum of 100 assets with value 10 should be 1000");
    }
    
} 