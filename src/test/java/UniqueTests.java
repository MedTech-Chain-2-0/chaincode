package nl.medtechchain.chaincode.test;

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

public class UniqueTests {
    private TestDataGenerator generator = new TestDataGenerator();

    private Query buildQuery(String field) {
        return Query.newBuilder()
            .setQueryType(Query.QueryType.UNIQUE_COUNT)
            .setTargetField(field)
            .build();
    }

    private long runUniqueCount(List<DeviceDataAsset> assets, String field) {
        Query query = buildQuery(field);
        PlatformConfig platformConfig = PlatformConfig.newBuilder().build();
        QueryService queryService = new QueryService(platformConfig, new DummyEncryption());
        QueryResult result = queryService.uniqueCount(query, assets);
        return result.getCountResult();
    }

    // string field plain values
    @Test
    public void stringFieldPlainValues() {
        Map<Object, Integer> counts = new HashMap<>();
        counts.put("Abc", 2);
        counts.put("Def", 3);
        counts.put("Ghi", 1);
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("hospital", counts);
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        
        long result = runUniqueCount(assets, "hospital");
        
        Assertions.assertEquals(3, result);
    }

    // string field encrypted values
    @Test
    public void stringFieldEncryptedValues() {
        TestDataGenerator.Cyphertext c1 = generator.new Cyphertext("Hospital X");
        TestDataGenerator.Cyphertext c2 = generator.new Cyphertext("Hospital Y");
        
        Map<Object, Integer> counts = new HashMap<>();
        counts.put(c1, 2);
        counts.put(c2, 3);
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("hospital", counts);
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        
        long result = runUniqueCount(assets, "hospital");
        
        Assertions.assertEquals(2, result);
    }

    // medical speciality plain values
    @Test
    public void medicalSpecialityPlainValues() {
        Map<Object, Integer> counts = new HashMap<>();
        counts.put(MedicalSpeciality.CARDIOLOGY, 2);
        counts.put(MedicalSpeciality.NEUROLOGY, 1);
        counts.put(MedicalSpeciality.ONCOLOGY, 3);
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("speciality", counts);
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        
        long result = runUniqueCount(assets, "speciality");
        
        Assertions.assertEquals(3, result);
    }

    // Test device category field plain values
    @Test
    public void deviceCategoryPlainValues() {
        Map<Object, Integer> counts = new HashMap<>();
        counts.put(DeviceCategory.PORTABLE, 2);
        counts.put(DeviceCategory.WEARABLE, 3);
        
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("category", counts);
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        long result = runUniqueCount(assets, "category");
        
        Assertions.assertEquals(2, result);
    }

    // empty dataset
    @Test
    public void emptyDataset() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("hospital", new HashMap<>());
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 0);
        
        long result = runUniqueCount(assets, "hospital");
        
        Assertions.assertEquals(0, result);
    }

    //mixed plain and encrypted values
    @Test
    public void mixedPlainAndEncryptedValues() {
        TestDataGenerator.Cyphertext c1 = generator.new Cyphertext("X");
        
        Map<Object, Integer> counts = new HashMap<>();
        counts.put("A", 2);
        counts.put(c1, 2);
        counts.put("B", 1);
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("hospital", counts);
        
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        
        long result = runUniqueCount(assets, "hospital");
        
        Assertions.assertEquals(3, result);
    }
}