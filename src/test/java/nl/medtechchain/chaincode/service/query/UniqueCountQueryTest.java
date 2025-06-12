package nl.medtechchain.chaincode.service.query;

import nl.medtechchain.chaincode.service.query.uniquecount.UniqueCountQuery;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceCategory;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

public class UniqueCountQueryTest {

    private TestDataGenerator generator;
    private PlatformConfig testConfig;

    @BeforeEach
    public void setup() {
        generator = new TestDataGenerator(11);
        testConfig = PlatformConfig.newBuilder().build();
    }

    private long execute(List<DeviceDataAsset> assets, String field, TestEncryptionService enc) {
        UniqueCountQuery q = new UniqueCountQuery(testConfig);
        if (enc != null) {
            try {
                var f = q.getClass().getSuperclass().getDeclaredField("encryptionService");
                f.setAccessible(true);
                f.set(q, enc);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        Query proto = Query.newBuilder()
                .setQueryType(Query.QueryType.UNIQUE_COUNT)
                .setTargetField(field)
                .build();
        QueryResult r = q.process(proto, assets);
        return r.getCountResult();
    }

    // Plaintext only
    @Test
    public void testPlaintextUniqueHospitals() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> hospitals = new HashMap<>();
        hospitals.put("HospitalX", 3);
        hospitals.put("HospitalY", 2);
        spec.put("hospital", hospitals);

        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        long count = execute(assets, "hospital", null);
        Assertions.assertEquals(2, count);
    }

    // Mixed plaintext + encrypted values
    @Test
    public void testMixedPlainAndEncryptedCategory() {
        Set<String> versions = Set.of("paillier-v1");
        TestEncryptionService enc = new TestEncryptionService(true, false, versions, "paillier-v1");

        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> cat = new HashMap<>();
        // plaintext PORTABLE
        cat.put(DeviceCategory.PORTABLE, 1);
        // encrypted PORTABLE (duplicate after decryption)
        cat.put(TestEncryptionService.encryptLong(DeviceCategory.PORTABLE.getNumber(), "paillier-v1"), 2);
        // encrypted WEARABLE
        cat.put(TestEncryptionService.encryptLong(DeviceCategory.WEARABLE.getNumber(), "paillier-v1"), 3);
        spec.put("category", cat);

        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 6);
        long count = execute(assets, "category", enc);
        Assertions.assertEquals(2, count, "Should have two distinct categories");
    }

    // Empty list
    @Test
    public void testEmptyAssets() {
        long c = execute(Collections.emptyList(), "hospital", null);
        Assertions.assertEquals(0, c);
    }

    // Encrypted but no EncryptionService configured
    @Test
    public void testEncryptedWithoutServiceThrows() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> map = new HashMap<>();
        map.put(TestEncryptionService.encryptLong(1), 1);
        spec.put("usage_hours", map);
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 1);

        Assertions.assertThrows(IllegalStateException.class,
                () -> execute(assets, "usage_hours", null));
    }

    // Same plaintext encrypted under different key versions
    @Test
    public void testMultiVersionSameValue() {
        Set<String> versions = Set.of("paillier-v1", "paillier-v2");
        TestEncryptionService enc = new TestEncryptionService(true, false, versions, "paillier-v1");

        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> hospitals = new HashMap<>();
        hospitals.put(TestEncryptionService.encryptLong("General".hashCode(), "paillier-v1"), 1);
        hospitals.put(TestEncryptionService.encryptLong("General".hashCode(), "paillier-v2"), 1);
        spec.put("hospital", hospitals);

        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 2);
        long count = execute(assets, "hospital", enc);
        Assertions.assertEquals(1, count, "Same decrypted value under different key versions should count once");
    }
} 