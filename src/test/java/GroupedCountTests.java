package nl.medtechchain.chaincode.test;

import com.google.protobuf.Timestamp;
import nl.medtechchain.chaincode.service.query.QueryService;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceCategory;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.devicedata.MedicalSpeciality;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupedCountTests {
    private TestDataGenerator generator = new TestDataGenerator();

    private Query buildQuery(String field) {
        return Query.newBuilder()
            .setQueryType(Query.QueryType.GROUPED_COUNT)
            .setTargetField(field)
            .build();
    }

    private Map<String, Long> runGroupedCount(List<DeviceDataAsset> assets, String field) {
        Query query = buildQuery(field);
        PlatformConfig platformConfig = PlatformConfig.newBuilder().build();
        QueryService queryService = new QueryService(platformConfig, new DummyEncryption());
        QueryResult result = queryService.groupedCount(query, assets);
        return result.getGroupedCountResult().getMapMap();
    }

    @Test
    public void stringFieldPlainValues() {
        Map<Object, Integer> counts = new HashMap<>();
        counts.put("A", 3);
        counts.put("B", 2);
        counts.put("C", 1);
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("hospital", counts);
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        
        Map<String, Long> result = runGroupedCount(assets, "hospital");
        
        Assertions.assertEquals(3, result.size(), "Should have 3 distinct groups");
        Assertions.assertEquals(3L, result.get("A"), "A count incorrect");
        Assertions.assertEquals(2L, result.get("B"), "B count incorrect");
        Assertions.assertEquals(1L, result.get("C"), "C count incorrect");
    }

    @Test
    public void integerFieldPlainValues() {
        Map<Object, Integer> counts = new HashMap<>();
        counts.put(10, 2);
        counts.put(20, 3);
        counts.put(30, 1);
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("usage_hours", counts);
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        
        Map<String, Long> result = runGroupedCount(assets, "usage_hours");
        
        Assertions.assertEquals(3, result.size(), "Should have 3 distinct groups");
        Assertions.assertEquals(2L, result.get("10"), "Count for value 10 incorrect");
        Assertions.assertEquals(3L, result.get("20"), "Count for value 20 incorrect");
        Assertions.assertEquals(1L, result.get("30"), "Count for value 30 incorrect");
    }

    @Test
    public void booleanFieldPlainValues() {
        Map<Object, Integer> counts = new HashMap<>();
        counts.put(true, 3);
        counts.put(false, 2);
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("active_status", counts);
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        
        Map<String, Long> result = runGroupedCount(assets, "active_status");
        
        Assertions.assertEquals(2, result.size(), "Should have 2 distinct groups");
        Assertions.assertEquals(3L, result.get("true"));
        Assertions.assertEquals(2L, result.get("false"));
    }

    @Test
    public void medicalSpecialityPlainValues() {
        Map<Object, Integer> counts = new HashMap<>();
        counts.put(MedicalSpeciality.CARDIOLOGY, 2);
        counts.put(MedicalSpeciality.NEUROLOGY, 3);
        counts.put(MedicalSpeciality.ONCOLOGY, 1);
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("speciality", counts);
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        
        Map<String, Long> result = runGroupedCount(assets, "speciality");
        
        Assertions.assertEquals(3, result.size(), "Should have 3 distinct groups");
        Assertions.assertEquals(2L, result.get("CARDIOLOGY"), "Count for CARDIOLOGY incorrect");
        Assertions.assertEquals(3L, result.get("NEUROLOGY"), "Count for NEUROLOGY incorrect");
        Assertions.assertEquals(1L, result.get("ONCOLOGY"), "Count for ONCOLOGY incorrect");
    }

    @Test
    public void deviceCategoryPlainValues() {
        Map<Object, Integer> counts = new HashMap<>();
        counts.put(DeviceCategory.PORTABLE, 2);
        counts.put(DeviceCategory.WEARABLE, 3);
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("category", counts);
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        
        Map<String, Long> result = runGroupedCount(assets, "category");
        
        Assertions.assertEquals(2, result.size(), "Should have 2 distinct groups");
        Assertions.assertEquals(2L, result.get("PORTABLE"), "Count for PORTABLE incorrect");
        Assertions.assertEquals(3L, result.get("WEARABLE"), "Count for WEARABLE incorrect");
    }

    @Test
    public void timestampFieldPlainValues() {
        Timestamp t1 = Timestamp.newBuilder().setSeconds(1000).build();
        Timestamp t2 = Timestamp.newBuilder().setSeconds(2000).build();
        
        Map<Object, Integer> counts = new HashMap<>();
        counts.put(t1, 2);
        counts.put(t2, 3);
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("production_date", counts);
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        
        Map<String, Long> result = runGroupedCount(assets, "production_date");
        
        Assertions.assertEquals(2, result.size(), "Should have 2 distinct groups");
        Assertions.assertEquals(2L, result.get("1000"));
        Assertions.assertEquals(3L, result.get("2000"));
    }

    @Test
    public void encryptedStringValues() {
        TestDataGenerator.Cyphertext c1 = generator.new Cyphertext("X");
        TestDataGenerator.Cyphertext c2 = generator.new Cyphertext("Y");
        
        Map<Object, Integer> counts = new HashMap<>();
        counts.put(c1, 2);
        counts.put(c2, 3);
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("hospital", counts);
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        
        Map<String, Long> result = runGroupedCount(assets, "hospital");
        
        Assertions.assertEquals(2, result.size(), "Should have 2 distinct groups");
        Assertions.assertEquals(2L, result.get("X"), "Count for X incorrect");
        Assertions.assertEquals(3L, result.get("Y"), "Count for Y incorrect");
    }

    @Test
    public void encryptedIntegerValues() {
        TestDataGenerator.Cyphertext c1 = generator.new Cyphertext("100");
        TestDataGenerator.Cyphertext c2 = generator.new Cyphertext("200");
        
        Map<Object, Integer> counts = new HashMap<>();
        counts.put(c1, 2);
        counts.put(c2, 3);
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("usage_hours", counts);
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        
        Map<String, Long> result = runGroupedCount(assets, "usage_hours");
        
        Assertions.assertEquals(2, result.size(), "Should have 2 distinct groups");
        Assertions.assertEquals(2L, result.get("100"), "value 100 incorrect");
        Assertions.assertEquals(3L, result.get("200"), "value 200 incorrect");
    }

    @Test
    public void emptyDataset() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("hospital", new HashMap<>());
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 0);
        
        Map<String, Long> result = runGroupedCount(assets, "hospital");
        
        Assertions.assertTrue(result.isEmpty(), "Result should be empty for empty dataset");
    }

    @Test
    public void mixedPlainAndEncryptedValues() {
        TestDataGenerator.Cyphertext c1 = generator.new Cyphertext("X");
        
        Map<Object, Integer> counts = new HashMap<>();
        counts.put("river", 2);
        counts.put(c1, 2);
        counts.put("sea", 1);
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("hospital", counts);
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        
        Map<String, Long> result = runGroupedCount(assets, "hospital");
        
        Assertions.assertEquals(3, result.size(), "Should have 3 distinct groups");
        Assertions.assertEquals(2L, result.get("river"));
        Assertions.assertEquals(2L, result.get("X"));
        Assertions.assertEquals(1L, result.get("sea"));
    }
}