package nl.medtechchain.chaincode.service.query;

import com.google.protobuf.Timestamp;
import nl.medtechchain.chaincode.service.query.linearregression.LinearRegressionQuery;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

public class LinearRegressionQueryTest {

    private static final double SECONDS_PER_DAY = 86400;
    private static final double SLOPE_EPS = 0.1;
    private static final double SMALL_EPS = 0.0001;

    private TestDataGenerator generator;
    private PlatformConfig cfg;

    @BeforeEach
    void setUp() {
        generator = new TestDataGenerator(42);
        cfg = PlatformConfig.newBuilder().build();
    }
    // proto query
    private Query buildQuery() {
        return Query.newBuilder()
                .setQueryType(Query.QueryType.LINEAR_REGRESSION)
                .setXTargetField("production_date")
                .setYTargetField("usage_hours")
                .build();
    }

    private QueryResult.LinearRegressionResult run(List<DeviceDataAsset> assets,
                                                   TestEncryptionService encSvc) {
        LinearRegressionQuery lr = new LinearRegressionQuery(cfg);

        //encryption service if provided.
        if (encSvc != null) {
            try {
                var f = lr.getClass().getSuperclass().getDeclaredField("encryptionService");
                f.setAccessible(true);
                f.set(lr, encSvc);
            } catch (Exception e) {
                throw new RuntimeException("Reflection failed", e);
            }
        }
        return lr.process(buildQuery(), assets).getLinearRegressionResult();
    }

    private DeviceDataAsset plain(long x, long y) {
        Map<String, Object> fields = new HashMap<>();
        fields.put("production_date", Timestamp.newBuilder().setSeconds(x).build());
        fields.put("usage_hours", (int) y);
        return generator.generateAsset(fields);
    }

    private DeviceDataAsset asset(long x,
                                  long y,
                                  String version,
                                  boolean encX,
                                  boolean encY) {
        Map<String, Object> fields = new HashMap<>();

        if (encX) {
            TestDataGenerator.Ciphertext ctX = TestEncryptionService.encryptLong(x, version);
            fields.put("production_date", ctX);
        } else {
            fields.put("production_date",
                    Timestamp.newBuilder().setSeconds(x).build());
        }

        if (encY) {
            TestDataGenerator.Ciphertext ctY = TestEncryptionService.encryptLong(y, version);
            fields.put("usage_hours", ctY);
        } else {
            fields.put("usage_hours", (int) y);
        }
        return generator.generateAsset(fields, version);
    }

    @Test
    public void emptyAssetListReturnsZeros() {
        var r = run(Collections.emptyList(), null);
        Assertions.assertEquals(0, r.getSlope(), SMALL_EPS);
        Assertions.assertEquals(0, r.getIntercept(), SMALL_EPS);
        Assertions.assertEquals(0, r.getRmse(), SMALL_EPS);
    }

    @Test
    public void singlePointReturnsZeros() {
        List<DeviceDataAsset> assets = List.of(plain(1, 2));
        var r = run(assets, null);
        Assertions.assertEquals(0, r.getSlope(), SMALL_EPS);
        Assertions.assertEquals(0, r.getIntercept(), SMALL_EPS);
        Assertions.assertEquals(0, r.getRmse(), SMALL_EPS);
    }

    @Test
    public void perfectLinearCorrelationPlaintext() {
        List<DeviceDataAsset> assets = new ArrayList<>();
        for (int x = 1; x <= 4; x++)
            assets.add(plain(x, 2 * x + 1));

        var r = run(assets, null);
        Assertions.assertEquals(2 * SECONDS_PER_DAY, r.getSlope(), SLOPE_EPS);
        Assertions.assertEquals(1, r.getIntercept(), SMALL_EPS);
        Assertions.assertEquals(0, r.getRmse(), SMALL_EPS);  
    }

    @Test
    public void noCorrelationPlaintext() {
        List<DeviceDataAsset> assets = List.of(
                plain(1, 5),
                plain(2, 2),
                plain(3, 8),
                plain(4, 1));

        Assertions.assertTrue(run(assets, null).getRmse() > 2);  
    }

    @Test
    public void encryptedDataWithoutServiceThrows() {
        List<DeviceDataAsset> assets = List.of(
                asset(1, 3, "v1", true, true),
                asset(2, 5, "v1", true, true));

        Assertions.assertThrows(NullPointerException.class,
                () -> run(assets, null));
    }

    @Test
    public void paillierHomomorphic() {
        Set<String> versions = Set.of("paillier-v1");
        TestEncryptionService enc = new TestEncryptionService(true, true, versions, "paillier-v1");

        List<DeviceDataAsset> assets = new ArrayList<>();
        for (int x = 1; x <= 5; x++)
            assets.add(asset(x, 3 * x + 2, "paillier-v1", true, true));

        var r = run(assets, enc);
        Assertions.assertEquals(3 * SECONDS_PER_DAY, r.getSlope(), SLOPE_EPS);
        Assertions.assertEquals(2, r.getIntercept(), SMALL_EPS);
        Assertions.assertEquals(0, r.getRmse(), SMALL_EPS);
    }

    @Test
    public void mixedPlainAndEncrypted() {
        Set<String> versions = Set.of("paillier-v1");
        TestEncryptionService enc = new TestEncryptionService(true, true, versions, "paillier-v1");

        List<DeviceDataAsset> assets = List.of(
                plain(1, 3),
                asset(2, 5, "paillier-v1", true, true),
                plain(3, 7),
                asset(4, 9, "paillier-v1", true, true));

        var r = run(assets, enc);
        Assertions.assertEquals(2 * SECONDS_PER_DAY, r.getSlope(), SLOPE_EPS);
        Assertions.assertEquals(1, r.getIntercept(), SMALL_EPS);
        Assertions.assertEquals(0, r.getRmse(), SMALL_EPS); 
    }

    @Test
    public void weightedAverageAcrossVersions() {
        List<DeviceDataAsset> assets = new ArrayList<>();
        for (int x = 1; x <= 3; x++) {
            assets.add(plain(x, x));
            assets.add(asset(x, 3 * x + 2, "v2", false, false));
        }
        var r = run(assets, null);
        Assertions.assertEquals(2 * SECONDS_PER_DAY, r.getSlope(), SLOPE_EPS);
        Assertions.assertEquals(1, r.getIntercept(), SMALL_EPS);
        Assertions.assertTrue(r.getRmse() > 2); 
    }

    @Test
    public void versionWithSinglePointIgnored() {
        List<DeviceDataAsset> assets = new ArrayList<>();
        for (int x = 1; x <= 5; x++)
            assets.add(asset(x, 2 * x, "v1", false, false));

        assets.add(asset(10, 1234, "v2", false, false));

        var r = run(assets, null);
        Assertions.assertEquals(2 * SECONDS_PER_DAY, r.getSlope(), SLOPE_EPS);
        Assertions.assertEquals(0, r.getIntercept(), SMALL_EPS);
    }

    @Test
    public void largeDatasetPlaintext() {
        List<DeviceDataAsset> assets = new ArrayList<>();
        for (int x = 1; x <= 1_000; x++)
            assets.add(plain(x, 4 * x + 3));

        var r = run(assets, null);
        Assertions.assertEquals(4 * SECONDS_PER_DAY, r.getSlope(), SLOPE_EPS);
        Assertions.assertEquals(3, r.getIntercept(), SMALL_EPS);
        Assertions.assertEquals(0, r.getRmse(), SMALL_EPS);
    }

    @Test
    public void negativeSlopePlaintext() {
        List<DeviceDataAsset> assets = new ArrayList<>();
        for (int x = 1; x <= 5; x++)
            assets.add(plain(x, -2 * x + 10));

        var r = run(assets, null);
        Assertions.assertEquals(-2 * SECONDS_PER_DAY, r.getSlope(), SLOPE_EPS);
        Assertions.assertEquals(10, r.getIntercept(), SMALL_EPS);
        Assertions.assertEquals(0, r.getRmse(), SMALL_EPS);
    }
}
