package nl.medtechchain.chaincode.service.query;

import nl.medtechchain.chaincode.service.query.average.AverageQuery;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;
import com.google.protobuf.Timestamp;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;


public class AverageQueryTest {
    private TestDataGenerator generator;
    private PlatformConfig testConfig;
    
    @BeforeEach
    public void setUp() {
        generator = new TestDataGenerator(42); 
        testConfig = PlatformConfig.newBuilder().build();
    }

    private Query buildAverageQuery(String targetField) {
        return Query.newBuilder()
                .setQueryType(Query.QueryType.AVERAGE)
                .setTargetField(targetField)
                .build();
    }

    private double executeAverage(List<DeviceDataAsset> assets, String targetField) {
        AverageQuery averageQuery = new AverageQuery(testConfig);
        // inject test encryption service via reflection
        try {
            var encryptionServiceField = averageQuery.getClass().getSuperclass().getDeclaredField("encryptionService");
            encryptionServiceField.setAccessible(true);
            encryptionServiceField.set(averageQuery, new TestEncryptionService());
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject test encryption service", e);
        }
        
        Query query = buildAverageQuery(targetField);
        QueryResult result = averageQuery.process(query, assets);
        return result.getAverageResult();
    }

    //plaintext only test
    private double executeAveragePlaintextOnly(List<DeviceDataAsset> assets, String targetField) {
        AverageQuery averageQuery = new AverageQuery(testConfig);
        // leave encryption service as null
        
        Query query = buildAverageQuery(targetField);
        QueryResult result = averageQuery.process(query, assets);
        return result.getAverageResult();
    }

    @Test
    public void testSingleAsset() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(100, 1);
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 1);
        double result = executeAverage(assets, "usage_hours");
        
        Assertions.assertEquals(100.0, result, "Single asset average should be 100");
    }

    @Test
    public void testMultipleAssetsSameValue() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(50, 5); // 5 assets with value 50
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        double result = executeAverage(assets, "usage_hours");
        
        Assertions.assertEquals(50.0, result, "Average of 5 assets with value 50 should be 50");
    }

    @Test
    public void testMultipleAssetsDifferentValues() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(100, 3); // 3 assets 100
        usageHours.put(200, 2); // 2 assets 200
        usageHours.put(300, 1); // 1 asset 300
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 6);
        double result = executeAverage(assets, "usage_hours");
        
        double expected = (100.0 * 3 + 200.0 * 2 + 300.0 * 1) / 6; // 1000/6 = 166.67
        Assertions.assertEquals(expected, result, 0.001, "Average should be approximately 166.67");
    }

    @Test
    public void testEmptyAssetList() {
        List<DeviceDataAsset> assets = Collections.emptyList();
        double result = executeAverage(assets, "usage_hours");
        
        Assertions.assertEquals(0.0, result, "Average of empty list should be 0");
    }


    @Test
    public void testZeroValues() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(0, 3); // 3 assets 0
        usageHours.put(100, 2); // 2 assets 100
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        double result = executeAverage(assets, "usage_hours");
        
        Assertions.assertEquals(40.0, result, "Average should be 40");
    }

    @Test
    public void testLargeValues() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(1000000, 2); // 2 assets 1000000
        usageHours.put(2000000, 1); // 1 asset 2000000
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 3);
        double result = executeAverage(assets, "usage_hours");
        
        double expected = (1000000.0 * 2 + 2000000.0) / 3; // = 4,000,000/3 ≈ 1,333,333.33
        Assertions.assertEquals(expected, result, 0.001, "Large values should be handled correctly");
    }
    
    @Test
    public void testNegativeValues() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(-100, 2); // 2 assets -100
        usageHours.put(300, 1); // 1 assets 300
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 3);
        double result = executeAverage(assets, "usage_hours");
        
        double expected = (-100.0 * 2 + 300.0) / 3; // = 100/3 ≈ 33.33
        Assertions.assertEquals(expected, result, 0.001, "Negative values should be handled correctly");
    }

    @Test
    public void testPlaintextOnlyWithNullEncryptionService() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(100, 2);
        usageHours.put(200, 3);
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        double result = executeAveragePlaintextOnly(assets, "usage_hours");
        
        double expected = (100.0 * 2 + 200.0 * 3) / 5; // = 800/5 = 160
        Assertions.assertEquals(expected, result, "Plaintext data should work fine without encryption service");
    }
    
    @Test
    public void testEncryptedDataWithNullEncryptionServiceThrowsError() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(TestEncryptionService.encryptLong(100), 1);
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 1);
        
        IllegalStateException exception = Assertions.assertThrows(
            IllegalStateException.class,
            () -> executeAveragePlaintextOnly(assets, "usage_hours")
        );
        
        Assertions.assertTrue(exception.getMessage().contains("Found encrypted data but no encryption service configured"));
        Assertions.assertTrue(exception.getMessage().contains("Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME"));
    }

    @Test
    public void testPaillierEncryptedHomomorphic() {
        AverageQuery averageQuery = new AverageQuery(testConfig);
        
        // setup paillier encryption
        Set<String> versions = Set.of("paillier-v1");
        TestEncryptionService paillierService = new TestEncryptionService(true, false, versions, "paillier-v1");
        
        try {
            var encryptionServiceField = averageQuery.getClass().getSuperclass().getDeclaredField("encryptionService");
            encryptionServiceField.setAccessible(true);
            encryptionServiceField.set(averageQuery, paillierService);
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
        Query query = buildAverageQuery("usage_hours");
        QueryResult result = averageQuery.process(query, assets);
        
        double expected = (150.0 * 2 + 250.0 * 3) / 5; // = 1050/5 = 210
        Assertions.assertEquals(expected, result.getAverageResult(), "Paillier encrypted values should be processed correctly");
        
        // verify all assets are paillier encrypted
        for (DeviceDataAsset asset : assets) {
            Assertions.assertEquals("paillier-v1", asset.getKeyVersion());
            Assertions.assertEquals(DeviceDataAsset.IntegerField.FieldCase.ENCRYPTED, 
                    asset.getDeviceData().getUsageHours().getFieldCase());
        }
    }

    @Test
    public void testPurePlaintextData() {
        AverageQuery averageQuery = new AverageQuery(testConfig);
        TestEncryptionService plaintextService = new TestEncryptionService(false, false);
        
        try {
            var encryptionServiceField = averageQuery.getClass().getSuperclass().getDeclaredField("encryptionService");
            encryptionServiceField.setAccessible(true);
            encryptionServiceField.set(averageQuery, plaintextService);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject test encryption service", e);
        }
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(100, 3);
        usageHours.put(200, 2);
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        Query query = buildAverageQuery("usage_hours");
        QueryResult result = averageQuery.process(query, assets);
        
        double expected = (100.0 * 3 + 200.0 * 2) / 5;
        Assertions.assertEquals(expected, result.getAverageResult(), "Pure plaintext data should work correctly");
        
        for (DeviceDataAsset asset : assets) {
            Assertions.assertEquals("plaintext", asset.getKeyVersion());
            Assertions.assertEquals(DeviceDataAsset.IntegerField.FieldCase.PLAIN, 
                    asset.getDeviceData().getUsageHours().getFieldCase());
        }
    }
    
    @Test
    public void testPaillierEncryptedLargeDataset() {
        AverageQuery averageQuery = new AverageQuery(testConfig);
        Set<String> versions = Set.of("paillier-v1");
        TestEncryptionService paillierService = new TestEncryptionService(true, false, versions, "paillier-v1");
        
        try {
            var encryptionServiceField = averageQuery.getClass().getSuperclass().getDeclaredField("encryptionService");
            encryptionServiceField.setAccessible(true);
            encryptionServiceField.set(averageQuery, paillierService);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject test encryption service", e);
        }
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(TestEncryptionService.encryptLong(100, "paillier-v1"), 10);
        usageHours.put(TestEncryptionService.encryptLong(200, "paillier-v1"), 5);
        usageHours.put(TestEncryptionService.encryptLong(300, "paillier-v1"), 3);
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 18);
        Query query = buildAverageQuery("usage_hours");
        QueryResult result = averageQuery.process(query, assets);
        
        double expected = (100.0 * 10 + 200.0 * 5 + 300.0 * 3) / 18; // = 2900/18 ≈ 161.11
        Assertions.assertEquals(expected, result.getAverageResult(), 0.001, 
                "Paillier encryption should handle larger datasets efficiently");
    }
    
    // ================ Different Field Tests ================
    
    @Test
    public void testProductionDateField() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> productionDate = new HashMap<>();
        
        // timestamps for different production dates
        Timestamp date1 = Timestamp.newBuilder().setSeconds(1609459200).build(); // 2021-01-01
        Timestamp date2 = Timestamp.newBuilder().setSeconds(1640995200).build(); // 2022-01-01
        Timestamp date3 = Timestamp.newBuilder().setSeconds(1672531200).build(); // 2023-01-01
        
        productionDate.put(date1, 3); 
        productionDate.put(date2, 2);
        productionDate.put(date3, 1); 
        spec.put("production_date", productionDate);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 6);
        double result = executeAverage(assets, "production_date");
        
    
        double expected = (date1.getSeconds() * 3 + date2.getSeconds() * 2 + date3.getSeconds() * 1) / 6.0;
        Assertions.assertEquals(expected, result, 0.001, "Production date average should be correct");
    }
    
    @Test
    public void testWarrantyExpiryDateField() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> warrantyDate = new HashMap<>();
        
        // timestamps for different warranty expiry dates
        Timestamp date1 = Timestamp.newBuilder().setSeconds(1735689600).build(); // 2025-01-01
        Timestamp date2 = Timestamp.newBuilder().setSeconds(1767225600).build(); // 2026-01-01
        Timestamp date3 = Timestamp.newBuilder().setSeconds(1798761600).build(); // 2027-01-01
        
        warrantyDate.put(date1, 4); // 4 devices expiring in 2025
        warrantyDate.put(date2, 3); // 3 devices expiring in 2026
        warrantyDate.put(date3, 2); // 2 devices expiring in 2027
        spec.put("warranty_expiry_date", warrantyDate);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 9);
        double result = executeAverage(assets, "warranty_expiry_date");
        
        // Calculate expected average timestamp in seconds
        double expected = (date1.getSeconds() * 4 + date2.getSeconds() * 3 + date3.getSeconds() * 2) / 9.0;
        Assertions.assertEquals(expected, result, 0.001, "Warranty expiry date average should be correct");
    }
    
    
    
    @Test
    public void testEncryptedTimestampFields() {
        AverageQuery averageQuery = new AverageQuery(testConfig);
        Set<String> versions = Set.of("paillier-v1");
        TestEncryptionService paillierService = new TestEncryptionService(true, false, versions, "paillier-v1");
        
        try {
            var encryptionServiceField = averageQuery.getClass().getSuperclass().getDeclaredField("encryptionService");
            encryptionServiceField.setAccessible(true);
            encryptionServiceField.set(averageQuery, paillierService);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject test encryption service", e);
        }
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> productionDate = new HashMap<>();
        
        // encrypted timestamps
        Timestamp date1 = Timestamp.newBuilder().setSeconds(1609459200).build(); // 2021-01-01
        Timestamp date2 = Timestamp.newBuilder().setSeconds(1640995200).build(); // 2022-01-01
        
        productionDate.put(TestEncryptionService.encryptLong(date1.getSeconds(), "paillier-v1"), 3);
        productionDate.put(TestEncryptionService.encryptLong(date2.getSeconds(), "paillier-v1"), 2);
        spec.put("production_date", productionDate);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        Query query = buildAverageQuery("production_date");
        QueryResult result = averageQuery.process(query, assets);
        
        double expected = (date1.getSeconds() * 3 + date2.getSeconds() * 2) / 5.0;
        Assertions.assertEquals(expected, result.getAverageResult(), 0.001, 
                "Encrypted timestamp average should be correct");
    }
    
    // ================ Paillier Version Tests ================
    
    @Test
    public void testPaillierVersionGrouping() {
        AverageQuery averageQuery = new AverageQuery(testConfig);
        Set<String> versions = Set.of("paillier-v1", "paillier-v2");
        TestEncryptionService multiVersionService = new TestEncryptionService(true, false, versions, "paillier-v2");
        
        try {
            var encryptionServiceField = averageQuery.getClass().getSuperclass().getDeclaredField("encryptionService");
            encryptionServiceField.setAccessible(true);
            encryptionServiceField.set(averageQuery, multiVersionService);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject test encryption service", e);
        }
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(TestEncryptionService.encryptLong(100, "paillier-v1"), 3);
        usageHours.put(TestEncryptionService.encryptLong(200, "paillier-v2"), 2);
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        Query query = buildAverageQuery("usage_hours");
        QueryResult result = averageQuery.process(query, assets);
        
        double expected = (100.0 * 3 + 200.0 * 2) / 5; // = 700/5 = 140
        Assertions.assertEquals(expected, result.getAverageResult(), 
                "Should correctly handle multiple Paillier versions");
        
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
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(10, 100); // 100 assets with value 10
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 100);
        double result = executeAverage(assets, "usage_hours");
        
        Assertions.assertEquals(10.0, result, "Average of 100 assets with value 10 should be 10");
    }

    // ================ Additional Edge Cases ================
    
    @Test
    public void testMixedEncryptedAndPlaintext() {
        AverageQuery averageQuery = new AverageQuery(testConfig);
        Set<String> versions = Set.of("paillier-v1");
        TestEncryptionService mixedService = new TestEncryptionService(true, false, versions, "paillier-v1");
        
        try {
            var encryptionServiceField = averageQuery.getClass().getSuperclass().getDeclaredField("encryptionService");
            encryptionServiceField.setAccessible(true);
            encryptionServiceField.set(averageQuery, mixedService);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject test encryption service", e);
        }
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
  
        usageHours.put(100, 2); // plaintext
        usageHours.put(TestEncryptionService.encryptLong(200, "paillier-v1"), 3); // encrypted
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        Query query = buildAverageQuery("usage_hours");
        QueryResult result = averageQuery.process(query, assets);
        
        double expected = (100.0 * 2 + 200.0 * 3) / 5; // = 800/5 = 160
        Assertions.assertEquals(expected, result.getAverageResult(), 
                "Should correctly handle mix of encrypted and plaintext values");
    }

    @Test
    public void testInvalidFieldName() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(100, 1);
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 1);
        
        Exception exception = Assertions.assertThrows(
            Exception.class,
            () -> executeAverage(assets, "non_existent_field")
        );
        Assertions.assertNotNull(exception, "Should throw exception for invalid field name");
    }

    @Test
    public void testNullFieldValues() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(100, 3); // 3 100
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 3);

        assets.set(0, DeviceDataAsset.newBuilder()
            .setDeviceData(DeviceDataAsset.DeviceData.newBuilder().build())
            .build());
        
        double result = executeAverage(assets, "usage_hours");

        double expected = (100.0 * 2) / 2; // = 100
        Assertions.assertEquals(expected, result, "Should handle null values by ignoring them");
    }

    @Test
    public void testBoundaryValues() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(1000000000, 1);  // 1 billion
        usageHours.put(-1000000000, 1); // -1 billion
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 2);
        double result = executeAverage(assets, "usage_hours");
        
        Assertions.assertEquals(0.0, result, "Should handle large positive and negative values correctly");
    }

    @Test
    public void testTimestampEdgeCases() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> productionDate = new HashMap<>();
        
        Timestamp epochStart = Timestamp.newBuilder().setSeconds(0).build();
        Timestamp farFuture = Timestamp.newBuilder().setSeconds(4102444800L).build();
        Timestamp prePoch = Timestamp.newBuilder().setSeconds(-31536000L).build(); // 1969-01-01
        
        productionDate.put(epochStart, 1);
        productionDate.put(farFuture, 1);
        productionDate.put(prePoch, 1);
        spec.put("production_date", productionDate);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 3);
        double result = executeAverage(assets, "production_date");
        
        double expected = (epochStart.getSeconds() + farFuture.getSeconds() + prePoch.getSeconds()) / 3.0;
        Assertions.assertEquals(expected, result, 0.001, 
                "Should handle timestamp edge cases correctly");
    }

    @Test
    public void testVeryLargeDataset() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        

        usageHours.put(100, 400); 
        usageHours.put(200, 300); 
        usageHours.put(300, 200); 
        usageHours.put(400, 100); 
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 1000);
        double result = executeAverage(assets, "usage_hours");
        
        double expected = (100.0 * 400 + 200.0 * 300 + 300.0 * 200 + 400.0 * 100) / 1000;
        Assertions.assertEquals(expected, result, 0.001, 
                "Should handle very large datasets efficiently");
    }

    @Test
    public void testMultipleEncryptionSchemes() {
        AverageQuery averageQuery = new AverageQuery(testConfig);
        Set<String> versions = Set.of("paillier-v1", "paillier-v2", "aes-v1");
        TestEncryptionService multiSchemeService = new TestEncryptionService(true, true, versions, "paillier-v2");
        
        try {
            var encryptionServiceField = averageQuery.getClass().getSuperclass().getDeclaredField("encryptionService");
            encryptionServiceField.setAccessible(true);
            encryptionServiceField.set(averageQuery, multiSchemeService);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject test encryption service", e);
        }
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(TestEncryptionService.encryptLong(100, "paillier-v1"), 2);
        usageHours.put(TestEncryptionService.encryptLong(200, "paillier-v2"), 2);
        usageHours.put(TestEncryptionService.encryptLong(300, "aes-v1"), 1);
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        Query query = buildAverageQuery("usage_hours");
        QueryResult result = averageQuery.process(query, assets);
        
        double expected = (100.0 * 2 + 200.0 * 2 + 300.0) / 5; // = 180
        Assertions.assertEquals(expected, result.getAverageResult(), 
                "Should handle multiple encryption schemes correctly");
    }

    @Test
    public void testDecimalValues() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        
    
        usageHours.put(1005, 2); 
        usageHours.put(2575, 2); 
        usageHours.put(15025, 1);
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        double result = executeAverage(assets, "usage_hours");
        
        // (10.05 * 2 + 25.75 * 2 + 150.25) / 5
        double expected = (1005.0 * 2 + 2575.0 * 2 + 15025.0) / 5;
        Assertions.assertEquals(expected, result, 0.001, 
                "Should handle decimal values correctly");
    }

    @Test
    public void testAllNullValues() {
        List<DeviceDataAsset> assets = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            assets.add(DeviceDataAsset.newBuilder()
                .setDeviceData(DeviceDataAsset.DeviceData.newBuilder().build())
                .build());
        }
        
        double result = executeAverage(assets, "usage_hours");
        
        Assertions.assertEquals(0.0, result, 
                "Should return 0 when all values are null");
    }
}
