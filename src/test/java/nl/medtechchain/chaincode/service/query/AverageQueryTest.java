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
        
        double expected = (100.0 * 3 + 200.0 * 2) / 5; // = 700/5 = 140
        Assertions.assertEquals(expected, result.getAverageResult(), "Pure plaintext data should work correctly");
        
        // Verify all assets are plaintext
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
        
        productionDate.put(date1, 3); // 3 devices from 2021
        productionDate.put(date2, 2); // 2 devices from 2022
        productionDate.put(date3, 1); // 1 device from 2023
        spec.put("production_date", productionDate);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 6);
        double result = executeAverage(assets, "production_date");
        
        // Calculate expected average timestamp in seconds
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
    public void testMixedTimestampFields() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        
        // Production dates
        Map<Object, Integer> productionDate = new HashMap<>();
        Timestamp prodDate1 = Timestamp.newBuilder().setSeconds(1609459200).build(); // 2021-01-01
        Timestamp prodDate2 = Timestamp.newBuilder().setSeconds(1640995200).build(); // 2022-01-01
        productionDate.put(prodDate1, 2);
        productionDate.put(prodDate2, 2);
        spec.put("production_date", productionDate);
        
        // Warranty dates
        Map<Object, Integer> warrantyDate = new HashMap<>();
        Timestamp warrantyDate1 = Timestamp.newBuilder().setSeconds(1735689600).build(); // 2025-01-01
        Timestamp warrantyDate2 = Timestamp.newBuilder().setSeconds(1767225600).build(); // 2026-01-01
        warrantyDate.put(warrantyDate1, 2);
        warrantyDate.put(warrantyDate2, 2);
        spec.put("warranty_expiry_date", warrantyDate);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 4);
        
        // Test production date average
        double prodResult = executeAverage(assets, "production_date");
        double expectedProd = (prodDate1.getSeconds() * 2 + prodDate2.getSeconds() * 2) / 4.0;
        Assertions.assertEquals(expectedProd, prodResult, 0.001, "Production date average should be correct");
        
        // Test warranty date average
        double warrantyResult = executeAverage(assets, "warranty_expiry_date");
        double expectedWarranty = (warrantyDate1.getSeconds() * 2 + warrantyDate2.getSeconds() * 2) / 4.0;
        Assertions.assertEquals(expectedWarranty, warrantyResult, 0.001, "Warranty expiry date average should be correct");
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
        
        // Create encrypted timestamps
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

    
    
    
}
