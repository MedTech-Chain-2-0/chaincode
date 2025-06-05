package nl.medtechchain.chaincode.test;

import nl.medtechchain.chaincode.service.query.QueryService;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StdTests {
    private TestDataGenerator generator = new TestDataGenerator();

    private Query buildQuery(String field) {
        return Query.newBuilder()
            .setQueryType(Query.QueryType.STD)
            .setTargetField(field)
            .build();
    }

    private QueryResult.MeanAndStd runStd(List<DeviceDataAsset> assets, String field) {
        Query query = buildQuery(field);
        PlatformConfig platformConfig = PlatformConfig.newBuilder().build();
        QueryService queryService = new QueryService(platformConfig, new DummyEncryption());
        QueryResult result = queryService.std(query, assets);
        return result.getMeanStd();
    }

    // Test single value -
    @Test
    public void singlePlainValue() {
        Map<String, Object> overrides = Map.of("usage_hours", 100);
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssets(overrides, 1);
        
        QueryResult.MeanAndStd result = runStd(assets, "usage_hours");
        
        Assertions.assertEquals(100.0, result.getMean());
        Assertions.assertEquals(0.0, result.getStd());
    }

    // Test multiple values
    @Test
    public void multiplePlainValues() {
        Map<Object, Integer> counts = new HashMap<>();
        counts.put(2, 1);
        counts.put(4, 2);
        counts.put(6, 1);
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("usage_hours", counts);
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        
        QueryResult.MeanAndStd result = runStd(assets, "usage_hours");
        
        Assertions.assertEquals(4.0, result.getMean());
        Assertions.assertEquals(Math.sqrt(8.0/3.0), result.getStd());
    }

    // Test encrypted values
    @Test
    public void encryptedValues() {
        TestDataGenerator.Cyphertext c1 = generator.new Cyphertext("2");
        TestDataGenerator.Cyphertext c2 = generator.new Cyphertext("4");
        
        Map<Object, Integer> counts = new HashMap<>();
        counts.put(c1, 2);
        counts.put(c2, 2);
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("usage_hours", counts);
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        
        QueryResult.MeanAndStd result = runStd(assets, "usage_hours");
        
        Assertions.assertEquals(3.0, result.getMean());
        Assertions.assertEquals(Math.sqrt(4.0/3.0), result.getStd());
    }

    // Test empty dataset
    @Test
    public void emptyDataset() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("usage_hours", new HashMap<>());
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 0);
        
        QueryResult.MeanAndStd result = runStd(assets, "usage_hours");
        
        Assertions.assertEquals(0.0, result.getMean());
        Assertions.assertEquals(0.0, result.getStd());
    }

    @Test
    public void singleValueDataset() {
        Map<Object, Integer> counts = new HashMap<>();
        counts.put(5, 3);
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("usage_hours", counts);
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        
        QueryResult.MeanAndStd result = runStd(assets, "usage_hours");
        
        Assertions.assertEquals(5.0, result.getMean());
        Assertions.assertEquals(0.0, result.getStd());
    }
}