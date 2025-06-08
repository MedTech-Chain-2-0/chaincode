package nl.medtechchain.chaincode.service.query;

import nl.medtechchain.chaincode.service.query.count.CountQuery;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

// Tests for CountQuery - total asset counting
public class CountQueryTest {
    
    private TestDataGenerator generator;
    private PlatformConfig testConfig;
    
    @BeforeEach
    public void setUp() {
        generator = new TestDataGenerator(42);
        testConfig = PlatformConfig.newBuilder().build();
    }
    
    private Query buildCountQuery(String targetField) {
        return Query.newBuilder()
                .setQueryType(Query.QueryType.COUNT)
                .setTargetField(targetField)
                .build();
    }
    
    private long executeCount(List<DeviceDataAsset> assets, String targetField) {
        CountQuery countQuery = new CountQuery(testConfig);
        Query query = buildCountQuery(targetField);
        QueryResult result = countQuery.process(query, assets);
        return result.getCountResult();
    }
    
    @Test
    public void testCountEmpty() {
        List<DeviceDataAsset> assets = Collections.emptyList();
        long result = executeCount(assets, "udi");
        
        Assertions.assertEquals(0, result, "Empty list should have count 0");
    }
    
    @Test
    public void testCountSingle() {
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(new HashMap<>(), 1);
        long result = executeCount(assets, "udi");
        
        Assertions.assertEquals(1, result, "Single asset should have count 1");
    }
    
    @Test
    public void testCountMultiple() {
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(new HashMap<>(), 10);
        long result = executeCount(assets, "udi");
        
        Assertions.assertEquals(10, result, "Should count all assets");
    }
    
    @Test
    public void testCountLarge() {
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(new HashMap<>(), 1000);
        long result = executeCount(assets, "udi");
        
        Assertions.assertEquals(1000, result, "Should handle large asset counts");
    }
    
    @Test
    public void testCountIgnoresTargetField() {
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(new HashMap<>(), 5);
        
        // Count should be the same regardless of target field
        long udiCount = executeCount(assets, "udi");
        long hospitalCount = executeCount(assets, "hospital");
        long batteryCount = executeCount(assets, "battery_level");
        
        Assertions.assertEquals(5, udiCount, "UDI count should be 5");
        Assertions.assertEquals(5, hospitalCount, "Hospital count should be 5");
        Assertions.assertEquals(5, batteryCount, "Battery count should be 5");
        
        Assertions.assertEquals(udiCount, hospitalCount, "All counts should be equal");
        Assertions.assertEquals(hospitalCount, batteryCount, "All counts should be equal");
    }
    
    @Test
    public void testCountWithMixedData() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> hospitals = new HashMap<>();
        hospitals.put("Hospital A", 3);
        hospitals.put("Hospital B", 2);
        spec.put("hospital", hospitals);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        long result = executeCount(assets, "hospital");
        
        Assertions.assertEquals(5, result, "Should count total assets, not distinct values");
    }
} 