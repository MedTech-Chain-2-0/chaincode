package nl.medtechchain.chaincode.test;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import nl.medtechchain.chaincode.service.encryption.PlatformEncryptionInterface;
import nl.medtechchain.chaincode.service.query.histogram.Histogram;
import nl.medtechchain.chaincode.service.query.histogram.IntegerHistogram;
import nl.medtechchain.chaincode.service.query.histogram.TimestampHistogram;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.devicedata.DeviceDataFieldType;
import nl.medtechchain.proto.query.QueryResult;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.chaincode.service.query.QueryService;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.chaincode.test.DummyEncryption;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

public class HistogramTests {
    private TestDataGenerator generator = new TestDataGenerator();

    private Query buildQuery(String field, long binSize) {
        return Query.newBuilder()
            .setQueryType(Query.QueryType.HISTOGRAM)
            .setTargetField(field)
            .setBinSize(binSize)
            .build();
    }

    private Map<String, Long> runHistogram(List<DeviceDataAsset> assets, String field, long binSize) {
        Query query = buildQuery(field, binSize);
        PlatformConfig platformConfig = PlatformConfig.newBuilder().build();
        
        QueryService queryService = new QueryService(platformConfig, new DummyEncryption());
        QueryResult result = queryService.histogram(query, assets);
        return result.getGroupedCountResult().getMapMap();
    }

    // integer plain values
    //
    @Test
    public void singlePlainValue() {
        Map<String, Object> overrides = Map.of("usage_hours", 17);
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssets(overrides, 1);
        Map<String, Long> res = runHistogram(assets, "usage_hours", 10);
        Assertions.assertEquals(1, res.size());
        Assertions.assertEquals(1L, res.get("10 - 19"));
    }

    @Test
    public void multiplePlainValues() {
        Map<Object, Integer> counts = new HashMap<>();
        counts.put(5, 2);
        counts.put(8, 1);
        counts.put(15, 1);
        counts.put(25, 3);
        counts.put(27, 8);

        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("battery_level", counts);

        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);

        Map<String, Long> result = runHistogram(assets, "battery_level", 10);

        Assertions.assertEquals(3, result.size()); //3 bins
        Assertions.assertEquals(3, result.get("0 - 9"));
        Assertions.assertEquals(1, result.get("10 - 19"));
        Assertions.assertEquals(11, result.get("20 - 29"));
    }

    // integer encrypted values
    //
    @Test
    public void integerEncryptedValue() {
        TestDataGenerator.Cyphertext c = generator.new Cyphertext("42");
        Map<String, Object> overrides = Map.of("usage_hours", c);
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssets(overrides, 1);

        Map<String, Long> resultMap = runHistogram(assets, "usage_hours", 10);

        Assertions.assertEquals(1, resultMap.size());
        Assertions.assertEquals(1L, resultMap.get("40 - 49"));
    }

    // plain timestamps
    @Test
    public void singleTimestampValue() {
        long seconds = 100;
        Timestamp ts = Timestamp.newBuilder().setSeconds(seconds).build();
        Map<String, Object> overrides = Map.of("production_date", ts);
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssets(overrides, 1);

        Map<String, Long> res = runHistogram(assets, "production_date", 1);
        Assertions.assertEquals(1, res.size());
        Assertions.assertEquals(1L, res.get("0 - 86399"));
    }

    @Test
    public void timestampMultipleValues() {
        long d0 = 50;
        long d1 = 86400 + 200;
        long d3 = 3 * 86400 + 10;
        Timestamp t0 = Timestamp.newBuilder().setSeconds(d0).build();
        Timestamp t1 = Timestamp.newBuilder().setSeconds(d1).build();
        Timestamp t3 = Timestamp.newBuilder().setSeconds(d3).build();

        Map<Object, Integer> counts = new HashMap<>();
        counts.put(t0, 1);
        counts.put(t1, 2);
        counts.put(t3, 1);
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("production_date", counts);

        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);

        Map<String, Long> res = runHistogram(assets, "production_date", 2); // histogram works in days

        Assertions.assertEquals(2, res.size());
        Assertions.assertEquals(3L, res.get("0 - 172799"));
        Assertions.assertEquals(1L, res.get("172800 - 345599"));
    }

    // timestamp encrypted
    @Test
    public void timestampEncrypted() {
        TestDataGenerator.Cyphertext c = generator.new Cyphertext(Long.toString(86400 + 5));
        Map<String, Object> overrides = new HashMap<>();
        overrides.put("production_date", c);
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssets(overrides, 1);

        Map<String, Long> res = runHistogram(assets, "production_date", 1);

        Assertions.assertEquals(1, res.size());
        Assertions.assertEquals(1L, res.get("86400 - 172799"));
    }

    // factories
    @Test
    public void factory_returnsCorrectImplementation() {
        Assertions.assertTrue(Histogram.Factory.getInstance(DeviceDataFieldType.INTEGER) instanceof IntegerHistogram,
                "factory for integer field should return IntegerHistogram");
        Assertions.assertTrue(Histogram.Factory.getInstance(DeviceDataFieldType.TIMESTAMP) instanceof TimestampHistogram,
                "factory for timestamp field should return TimestampHistogram");
    }
}