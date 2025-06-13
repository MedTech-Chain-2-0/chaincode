package nl.medtechchain.chaincode.service.query;

import com.google.protobuf.Timestamp;
import nl.medtechchain.chaincode.service.query.histogram.HistogramQuery;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.devicedata.DeviceCategory;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.time.Instant;

import java.util.*;

public class HistogramQueryTest {

    private TestDataGenerator generator;
    private PlatformConfig testConfig;

    @BeforeEach
    public void setUp() {
        generator = new TestDataGenerator(7);
        testConfig = PlatformConfig.newBuilder().build();
    }

    private Map<String, Long> executeHistogram(List<DeviceDataAsset> assets, String field, TestEncryptionService enc, long binSize) {
        HistogramQuery histogram = new HistogramQuery(testConfig, binSize);
        if (enc != null) {
            try {
                var f = histogram.getClass().getSuperclass().getDeclaredField("encryptionService");
                f.setAccessible(true);
                f.set(histogram, enc);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        Query q = Query.newBuilder()
                .setQueryType(Query.QueryType.HISTOGRAM)
                .setTargetField(field)
                .setBinSize(binSize)
                .build();
        QueryResult r = histogram.process(q, assets);
        return r.getGroupedCountResult().getMapMap();
    }

    @Test
    public void testPlaintextUsageHoursHistogram() {
        // 3 assets with usage_hours 100, 2 with usage_hours 203, 2 with usage_hours 250, 4 with usage_hours 300
        Map<String, Map<Object, Integer>> spec = new HashMap<>(); // Map of field name to map 
        Map<Object, Integer> usage_hours = new HashMap<>();
        usage_hours.put(100, 3);
        usage_hours.put(203, 2);
        usage_hours.put(250, 2);
        usage_hours.put(300, 4);
        spec.put("usage_hours", usage_hours);

        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 11);
        Map<String, Long> grouped = executeHistogram(assets, "usage_hours", null, 100);

        Assertions.assertEquals(3, grouped.getOrDefault("100-199", 0L));
        Assertions.assertEquals(4, grouped.getOrDefault("200-299", 0L));
        Assertions.assertEquals(4, grouped.getOrDefault("300-399", 0L));
        Assertions.assertEquals(3, grouped.size());
    }
    @Test
    public void testPlaintextBatteryLevelHistogram() {
        // 3 assets with battery_level 23, 1 with battery_level 54, 5 with battery_level 67, 6 with battery_level 90
        Map<String, Map<Object, Integer>> spec = new HashMap<>(); // Map of field name to map 
        Map<Object, Integer> battery_level = new HashMap<>();
        battery_level.put(23, 3);
        battery_level.put(54, 1);
        battery_level.put(67, 5);
        battery_level.put(90, 6);
        spec.put("battery_level", battery_level);

        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 15);
        Map<String, Long> grouped = executeHistogram(assets, "battery_level", null, 25);

        Assertions.assertEquals(3, grouped.getOrDefault("23-47", 0L));
        Assertions.assertEquals(6, grouped.getOrDefault("48-72", 0L));
        Assertions.assertEquals(6, grouped.getOrDefault("73-97", 0L));
        Assertions.assertEquals(3, grouped.size());
    }
    @Test
    public void testPlaintextProductionDateHistogram() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> productionDates = new HashMap<>();

        // All times are expressed in epoch seconds
        long baseDay = 1700000000L; // Some arbitrary starting timestamp

        productionDates.put(Timestamp.newBuilder().setSeconds(baseDay).build(), 2);
        productionDates.put(Timestamp.newBuilder().setSeconds(baseDay + 86400).build(), 3);
        productionDates.put(Timestamp.newBuilder().setSeconds(baseDay + 86400 * 2).build(), 1);

        spec.put("production_date", productionDates);

        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 6);

        // binSize = 1 means group by day → internally multiplied by 86400 seconds
        Map<String, Long> grouped = executeHistogram(assets, "production_date", null, 1);

        Assertions.assertEquals(2, grouped.getOrDefault(String.format("%d-%d", baseDay, baseDay + 86400 - 1), 0L));
        Assertions.assertEquals(3, grouped.getOrDefault(String.format("%d-%d", baseDay + 86400, baseDay + 86400 * 2 - 1), 0L));
        Assertions.assertEquals(1, grouped.getOrDefault(String.format("%d-%d", baseDay + 86400 * 2, baseDay + 86400 * 3 - 1), 0L));

        Assertions.assertEquals(3, grouped.size());
    }

    @Test
    public void testPlaintextLastServiceDateHistogram() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> lastServiceDates = new HashMap<>();

        long baseDay = 1705000000L; // arbitrary starting timestamp (epoch seconds)
        // 1 asset on baseDay
        lastServiceDates.put(Timestamp.newBuilder().setSeconds(baseDay).build(), 1);
        // 2 assets on baseDay + 1 day
        lastServiceDates.put(Timestamp.newBuilder().setSeconds(baseDay + 86400).build(), 2);
        // 3 assets on baseDay + 2 days
        lastServiceDates.put(Timestamp.newBuilder().setSeconds(baseDay + 86400 * 2).build(), 3);

        spec.put("last_service_date", lastServiceDates);

        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 6);

        Map<String, Long> grouped = executeHistogram(assets, "last_service_date", null, 1); // 1-day bins

        Assertions.assertEquals(1, grouped.getOrDefault(String.format("%d-%d", baseDay, baseDay + 86400 - 1), 0L));
        Assertions.assertEquals(2, grouped.getOrDefault(String.format("%d-%d", baseDay + 86400, baseDay + 86400 * 2 - 1), 0L));
        Assertions.assertEquals(3, grouped.getOrDefault(String.format("%d-%d", baseDay + 86400 * 2, baseDay + 86400 * 3 - 1), 0L));

        Assertions.assertEquals(3, grouped.size());
    }

    @Test
    public void testMixedEncryptedAndPlainUsageHours() {
        // 2 plaintext PORTABLE, 3 encrypted WEARABLE (Paillier)
        Set<String> versions = Set.of("paillier-v1");
        TestEncryptionService enc = new TestEncryptionService(true, false, versions, "paillier-v1");

        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usage = new HashMap<>();

        // Plaintext values
        usage.put(100, 2); // falls into bin 100-199

        // Encrypted values
        usage.put(TestEncryptionService.encryptLong(203L, "paillier-v1"), 3); // falls into bin 200-299

        spec.put("usage_hours", usage);

        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        Map<String, Long> grouped = executeHistogram(assets, "usage_hours", enc, 100); // bin size: 100

        Assertions.assertEquals(2, grouped.getOrDefault("100-199", 0L));
        Assertions.assertEquals(3, grouped.getOrDefault("200-299", 0L));
        Assertions.assertEquals(2, grouped.size());
    }

    @Test
    public void testHistogramMixedPlaintextEncryptedProductionDate() {
        Set<String> versions = Set.of("paillier-v1");
        TestEncryptionService enc = new TestEncryptionService(true, false, versions, "paillier-v1");

        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> prodDates = new HashMap<>();

        // Epoch seconds (start of day UTC)
        long jan10 = LocalDate.of(2022, 1, 10).atStartOfDay(ZoneOffset.UTC).toEpochSecond(); // 1641772800
        long jan15 = LocalDate.of(2022, 1, 15).atStartOfDay(ZoneOffset.UTC).toEpochSecond(); // 1642204800
        long jan20 = LocalDate.of(2022, 1, 20).atStartOfDay(ZoneOffset.UTC).toEpochSecond(); // 1642636800
        long jan30 = LocalDate.of(2022, 1, 30).atStartOfDay(ZoneOffset.UTC).toEpochSecond(); // 1643500800

        // Use protobuf Timestamps for plaintext values
        prodDates.put(Timestamp.newBuilder().setSeconds(jan10).build(), 2); // plaintext
        prodDates.put(TestEncryptionService.encryptLong(jan15, "paillier-v1"), 3); // encrypted
        prodDates.put(TestEncryptionService.encryptLong(jan20, "paillier-v1"), 2); // encrypted
        prodDates.put(Timestamp.newBuilder().setSeconds(jan30).build(), 1); // plaintext

        spec.put("production_date", prodDates);

        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 8);

        // Histogram step = 10 days (but you put 10 seconds in original, fix to seconds)
        // 10 days = 864000 seconds
        long step = 864000L;

        Map<String, Long> grouped = executeHistogram(assets, "production_date", enc, 10);

        Assertions.assertEquals(5L, grouped.getOrDefault(String.format("%d-%d", jan10, jan10 + step - 1 ), 0L));                // jan10 bin
        Assertions.assertEquals(2L, grouped.getOrDefault(String.format("%d-%d", jan10 + step, jan10 + 2 * step - 1), 0L));    // jan15 + jan20 bin
        Assertions.assertEquals(1L, grouped.getOrDefault(String.format("%d-%d", jan10 + 2 * step, jan10 + 3 * step - 1), 0L)); // jan30 bin
        Assertions.assertEquals(3, grouped.size());
    }


} 