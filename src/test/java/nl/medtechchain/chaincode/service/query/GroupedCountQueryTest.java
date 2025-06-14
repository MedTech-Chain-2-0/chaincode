package nl.medtechchain.chaincode.service.query;

import nl.medtechchain.chaincode.service.query.groupedcount.GroupedCountQuery;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.devicedata.DeviceCategory;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

public class GroupedCountQueryTest {

    private TestDataGenerator generator;
    private PlatformConfig testConfig;

    @BeforeEach
    public void setUp() {
        generator = new TestDataGenerator(7);
        testConfig = PlatformConfig.newBuilder().build();
    }

    private Map<String, Long> executeGrouped(List<DeviceDataAsset> assets, String field, TestEncryptionService enc) {
        GroupedCountQuery grouped = new GroupedCountQuery(testConfig);
        if (enc != null) {
            try {
                var f = grouped.getClass().getSuperclass().getDeclaredField("encryptionService");
                f.setAccessible(true);
                f.set(grouped, enc);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        Query q = Query.newBuilder()
                .setQueryType(Query.QueryType.GROUPED_COUNT)
                .setTargetField(field)
                .build();
        QueryResult r = grouped.process(q, assets);
        return r.getGroupedCountResult().getMapMap();
    }

    @Test
    public void testPlaintextHospitalGrouping() {
        // 3 assets from HospitalA, 2 from HospitalB
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> hospitals = new HashMap<>();
        hospitals.put("HospitalA", 3);
        hospitals.put("HospitalB", 2);
        spec.put("hospital", hospitals);

        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        Map<String, Long> grouped = executeGrouped(assets, "hospital", null);

        Assertions.assertEquals(3, grouped.getOrDefault("HospitalA", 0L));
        Assertions.assertEquals(2, grouped.getOrDefault("HospitalB", 0L));
        Assertions.assertEquals(2, grouped.size());
    }

    @Test
    public void testMixedEncryptedAndPlainCategory() {
        // 2 plaintext PORTABLE, 3 encrypted WEARABLE (Paillier)
        Set<String> versions = Set.of("paillier-v1");
        TestEncryptionService enc = new TestEncryptionService(true, false, versions, "paillier-v1");

        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> category = new HashMap<>();
        category.put(DeviceCategory.PORTABLE, 2);
        category.put(TestEncryptionService.encryptLong(DeviceCategory.WEARABLE.getNumber(), "paillier-v1"), 3);
        spec.put("category", category);

        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        Map<String, Long> grouped = executeGrouped(assets, "category", enc);

        Assertions.assertEquals(2, grouped.getOrDefault(DeviceCategory.PORTABLE.name(), 0L));
        Assertions.assertEquals(3, grouped.getOrDefault(DeviceCategory.WEARABLE.name(), 0L));
        Assertions.assertEquals(2, grouped.size());
    }
} 