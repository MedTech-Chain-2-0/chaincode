package nl.medtechchain.chaincode.test;

import com.google.protobuf.Timestamp;
import nl.medtechchain.chaincode.service.query.QueryService;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AverageTests {
    private TestDataGenerator generator = new TestDataGenerator();

    private Query buildQuery(String field) {
        return Query.newBuilder()
            .setQueryType(Query.QueryType.AVERAGE)
            .setTargetField(field)
            .build();
    }

    private double runAverage(List<DeviceDataAsset> assets, String field) {
        Query query = buildQuery(field);
        PlatformConfig platformConfig = PlatformConfig.newBuilder().build();
        QueryService queryService = new QueryService(platformConfig, new DummyEncryption());
        QueryResult result = queryService.average(query, assets);
        return result.getAverageResult();
    }

    @Test
    public void singlePlainIntegerValue() {
        Map<String, Object> overrides = Map.of("usage_hours", 100);
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssets(overrides, 1);
        
        double result = runAverage(assets, "usage_hours");
        
        Assertions.assertEquals(100.0, result, 0.001, "Average should equal the single value");
    }

    @Test
    public void multiplePlainIntegerValues() {
        Map<Object, Integer> counts = new HashMap<>();
        counts.put(10, 2);
        counts.put(20, 3);  
        counts.put(30, 1);  
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("usage_hours", counts);
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        
        double result = runAverage(assets, "usage_hours");
        
        double expected = (10.0*2 + 20.0*3 + 30.0*1) / (2+3+1);
        Assertions.assertEquals(expected, result, 0.50); // such delta, due to some imprecission, 
        // if precision was valuable we would have not been using e-diff noise for the results, thus acceptable
    }

    @Test
    public void encryptedIntegerValues() {
        TestDataGenerator.Cyphertext c1 = generator.new Cyphertext("5");
        TestDataGenerator.Cyphertext c2 = generator.new Cyphertext("15");
        
        Map<Object, Integer> counts = new HashMap<>();
        counts.put(c1, 2); 
        counts.put(c2, 2); 
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("usage_hours", counts);
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        
        double result = runAverage(assets, "usage_hours");
        
        double expected = (5.0*2 + 15.0*2) / 4;
        Assertions.assertEquals(expected, result, 0.50);
    }

    @Test
    public void plainTimestampValues() {
        Timestamp t1 = Timestamp.newBuilder().setSeconds(1000).build();
        Timestamp t2 = Timestamp.newBuilder().setSeconds(2000).build();
        
        Map<Object, Integer> counts = new HashMap<>();
        counts.put(t1, 2);  
        counts.put(t2, 3);  
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("production_date", counts);
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        
        double result = runAverage(assets, "production_date");
        
        double expected = (1000.0*2 + 2000.0*3) / 5;
        Assertions.assertEquals(expected, result, 0.50);
    }

    @Test
    public void encryptedTimestampValues() {
        TestDataGenerator.Cyphertext c1 = generator.new Cyphertext("1000");  
        TestDataGenerator.Cyphertext c2 = generator.new Cyphertext("3000");  
        
        Map<Object, Integer> counts = new HashMap<>();
        counts.put(c1, 2);  
        counts.put(c2, 1);  
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("production_date", counts);
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        
        double result = runAverage(assets, "production_date");
        
        double expected = (1000.0*2 + 3000.0*1) / (2+1);
        Assertions.assertEquals(expected, result, 0.50); 
    }

    @Test
    public void mixedPlainAndEncryptedIntegerValues() {
        TestDataGenerator.Cyphertext c1 = generator.new Cyphertext("10");
        
        Map<Object, Integer> counts = new HashMap<>();
        counts.put(5, 2);  
        counts.put(c1, 2);
        counts.put(15, 1);  
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("usage_hours", counts);
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        
        double result = runAverage(assets, "usage_hours");
        
        double expected = (5.0*2 + 10.0*2 + 15.0*1) / 5;
        Assertions.assertEquals(expected, result, 0.50); 
    }

    @Test
    public void homomorphicEncryptionSupport() {
        TestDataGenerator.Cyphertext c1 = generator.new Cyphertext("5");
        TestDataGenerator.Cyphertext c2 = generator.new Cyphertext("15");
        
        Map<Object, Integer> counts = new HashMap<>();
        counts.put(c1, 3);  
        counts.put(c2, 2);  
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("usage_hours", counts);
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        
        double result = runAverage(assets, "usage_hours");
        
        double expected = (5.0*3 + 15.0*2) / 5;
        Assertions.assertEquals(expected, result, 0.50); 
    }
}