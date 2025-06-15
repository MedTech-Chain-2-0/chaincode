package nl.medtechchain.chaincode.service.query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import nl.medtechchain.chaincode.service.query.standarddeviation.STDQuery;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;



public class STDQueryTest {
    
    private TestDataGenerator generator;
    private PlatformConfig testConfig;
    
    @BeforeEach
    public void setUp() {
        generator = new TestDataGenerator(42); // Fixed seed for reproducible tests
        testConfig = PlatformConfig.newBuilder().build();
    }
    
    // helper methods
    
    private Query buildSTDQuery(String targetField) {
        return Query.newBuilder()
                .setQueryType(Query.QueryType.STD)
                .setTargetField(targetField)
                .build();
    }

    
    private long executeSTD(List<DeviceDataAsset> assets, String targetField) {
        STDQuery stdQuery = new STDQuery(testConfig);

        // inject test encryption service via reflection
        try {
            var encryptionServiceField = stdQuery.getClass().getSuperclass().getDeclaredField("encryptionService");
            encryptionServiceField.setAccessible(true);
            encryptionServiceField.set(stdQuery, new TestEncryptionService());
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject test encryption service", e);
        }
        
        Query query = buildSTDQuery(targetField);
        QueryResult result = stdQuery.process(query, assets);
        return result.getSumResult();
    }
    

    
    @Test
    public void testSingleAsset() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(100, 1);
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 1);
        long result = executeSTD(assets, "usage_hours");
        
        Assertions.assertEquals(0, result, "Single asset sum should be 100");
    }


    
    @Test
    public void testPaillierEncryptedHomomorphic() {
        STDQuery stdQuery = new STDQuery(testConfig);
        
        // setup paillier encryption
        Set<String> versions = Set.of("paillier-v1");
        TestEncryptionService paillierService = new TestEncryptionService(true, false, versions, "paillier-v1");
        
        try {
            var encryptionServiceField = stdQuery.getClass().getSuperclass().getDeclaredField("encryptionService");
            encryptionServiceField.setAccessible(true);
            encryptionServiceField.set(stdQuery, paillierService);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject test encryption service", e);
        }
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        
        // only paillier encrypted values
        usageHours.put(TestEncryptionService.encryptLong(5, "paillier-v1"), 2);
        usageHours.put(TestEncryptionService.encryptLong(6, "paillier-v1"), 3);
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        Query query = buildSTDQuery("usage_hours");
        QueryResult result = stdQuery.process(query, assets);
        
        double mean = (5.0 * 2 + 3 * 6) / 5.0;
        double sum = 2 * (5 - mean) * (5 - mean) + 3 * (6 - mean) * (6 - mean);
        double expected = Math.sqrt(sum / 5); 
        Assertions.assertEquals(expected, result.getMeanStd().getStd(), 0.0001);
        
        // verify all assets are paillier encrypted
        for (DeviceDataAsset asset : assets) {
            Assertions.assertEquals("paillier-v1", asset.getKeyVersion());
            Assertions.assertEquals(DeviceDataAsset.IntegerField.FieldCase.ENCRYPTED, 
                    asset.getDeviceData().getUsageHours().getFieldCase());
        }
    }


    
    @Test
    public void testPaillierEncryptedHomomorphic_differentKeys() {
        STDQuery stdQuery = new STDQuery(testConfig);
        
        // setup paillier encryption
        Set<String> versions = Set.of("paillier-v1", "paillier-v2");
        TestEncryptionService paillierService = new TestEncryptionService(true, false, versions, "paillier-v2");
        
        try {
            var encryptionServiceField = stdQuery.getClass().getSuperclass().getDeclaredField("encryptionService");
            encryptionServiceField.setAccessible(true);
            encryptionServiceField.set(stdQuery, paillierService);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject test encryption service", e);
        }
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        
        // only paillier encrypted values
        usageHours.put(TestEncryptionService.encryptLong(5, "paillier-v1"), 2);
        usageHours.put(TestEncryptionService.encryptLong(6, "paillier-v2"), 3);
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        Query query = buildSTDQuery("usage_hours");
        QueryResult result = stdQuery.process(query, assets);
        
        double mean = (5.0 * 2 + 3 * 6) / 5.0;
        double sum = 2 * (5 - mean) * (5 - mean) + 3 * (6 - mean) * (6 - mean);
        double expected = Math.sqrt(sum / 5); 
        Assertions.assertEquals(expected, result.getMeanStd().getStd(), 0.0001);
        
        
    }

    
    @Test
    public void testPaillierEncryptedHomomorphic_noneAndPaillierTogether() {
        STDQuery stdQuery = new STDQuery(testConfig);
        
        // setup paillier encryption
        Set<String> versions = Set.of("paillier-v1");
        TestEncryptionService paillierService = new TestEncryptionService(true, false, versions, "paillier-v1");
        
        try {
            var encryptionServiceField = stdQuery.getClass().getSuperclass().getDeclaredField("encryptionService");
            encryptionServiceField.setAccessible(true);
            encryptionServiceField.set(stdQuery, paillierService);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject test encryption service", e);
        }
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        
        // plaintext and encrypted values together
        usageHours.put(TestEncryptionService.encryptLong(5, "paillier-v1"), 2);
        usageHours.put(6, 3);
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        Query query = buildSTDQuery("usage_hours");
        QueryResult result = stdQuery.process(query, assets);
        
        double mean = (5.0 * 2 + 3 * 6) / 5.0;
        double sum = 2 * (5 - mean) * (5 - mean) + 3 * (6 - mean) * (6 - mean);
        double expected = Math.sqrt(sum / 5); 
        Assertions.assertEquals(expected, result.getMeanStd().getStd(), 0.0001);
        
        
    }
    
    @Test
    public void testPaillierTimeStamp() {
        STDQuery stdQuery = new STDQuery(testConfig);
        
        // setup paillier encryption
        Set<String> versions = Set.of("paillier-v1");
        TestEncryptionService paillierService = new TestEncryptionService(true, false, versions, "paillier-v1");
        
        try {
            var encryptionServiceField = stdQuery.getClass().getSuperclass().getDeclaredField("encryptionService");
            encryptionServiceField.setAccessible(true);
            encryptionServiceField.set(stdQuery, paillierService);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject test encryption service", e);
        }
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> production_date = new HashMap<>();
        
        // only paillier encrypted values
        production_date.put(TestEncryptionService.encryptLong(5, "paillier-v1"), 2);
        production_date.put(TestEncryptionService.encryptLong(6, "paillier-v1"), 3);
        spec.put("production_date", production_date);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        Query query = buildSTDQuery("production_date");
        QueryResult result = stdQuery.process(query, assets);
        
        double mean = (5.0 * 2 + 3 * 6) / 5.0;
        double sum = 2 * (5 - mean) * (5 - mean) + 3 * (6 - mean) * (6 - mean);
        double expected = Math.sqrt(sum / 5); 
        Assertions.assertEquals(expected, result.getMeanStd().getStd(), 0.0001);
        
        // verify all assets are paillier encrypted
        for (DeviceDataAsset asset : assets) {
            Assertions.assertEquals("paillier-v1", asset.getKeyVersion());
            Assertions.assertEquals(DeviceDataAsset.TimestampField.FieldCase.ENCRYPTED, 
                    asset.getDeviceData().getProductionDate().getFieldCase());
        }
    }
} 