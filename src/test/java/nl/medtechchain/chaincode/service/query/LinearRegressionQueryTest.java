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

    private static final double SECONDS_PER_DAY = 86_400.0;
    private static final double SLOPE_EPS = 0.1;
    private static final double SMALL_EPS = 0.0001; 

    private TestDataGenerator generator;
    private PlatformConfig cfg;

    @BeforeEach
    public void init() {
        generator = new TestDataGenerator(42); 
        cfg = PlatformConfig.newBuilder().build();
    }

    private Query linearRegressionQuery(String xField, String yField) {
        return Query.newBuilder()
                .setQueryType(Query.QueryType.LINEAR_REGRESSION)
                .setXTargetField(xField)
                .setYTargetField(yField)
                .build();
    }

    private QueryResult.LinearRegressionResult exec(List<DeviceDataAsset> assets,
            String xField,
            String yField,
            TestEncryptionService encSvc) {
        LinearRegressionQuery lr = new LinearRegressionQuery(cfg);

        if (encSvc != null) {
            try {
                var f = lr.getClass().getSuperclass().getDeclaredField("encryptionService");
                f.setAccessible(true);
                f.set(lr, encSvc);
            } catch (Exception e) {
                throw new RuntimeException("reflection failed", e);
            }
        }
        return lr.process(linearRegressionQuery(xField, yField), assets).getLinearRegressionResult();
    }

    private DeviceDataAsset plainXY(long x, long y) {
        Map<String, Object> vals = new HashMap<>();
        vals.put("production_date", Timestamp.newBuilder().setSeconds(x).build());
        vals.put("usage_hours", (int) y);
        return generator.generateAsset(vals);
    }



    // tests
    @Test
    public void emptyAssetListGivesZeros() {
        QueryResult.LinearRegressionResult r = exec(new ArrayList<>(), "production_date", "usage_hours", null);

        Assertions.assertEquals(0.0, r.getSlope(), SMALL_EPS);
        Assertions.assertEquals(0.0, r.getIntercept(), SMALL_EPS);
        Assertions.assertEquals(0.0, r.getRSquared(), SMALL_EPS);
    }

    @Test
    public void singlePointGivesZeros() {
        List<DeviceDataAsset> assets = new ArrayList<>();
        assets.add(plainXY(1, 2));

        QueryResult.LinearRegressionResult r = exec(assets, "production_date", "usage_hours", null);

        Assertions.assertEquals(0.0, r.getSlope(), SMALL_EPS);
        Assertions.assertEquals(0.0, r.getIntercept(), SMALL_EPS);
        Assertions.assertEquals(0.0, r.getRSquared(), SMALL_EPS);
    }

    @Test
    public void perfectLinearCorrelationPlaintext() {
        List<DeviceDataAsset> assets = new ArrayList<>();
        for (int x = 1; x <= 4; x++) {
            assets.add(plainXY(x, 2 * x + 1));
        }

        QueryResult.LinearRegressionResult r = exec(assets, "production_date", "usage_hours", null);

        double expectedSlope = 2.0 * SECONDS_PER_DAY;
        Assertions.assertEquals(expectedSlope, r.getSlope(), SLOPE_EPS);
        Assertions.assertEquals(1.0, r.getIntercept(), SMALL_EPS);
        Assertions.assertEquals(1.0, r.getRSquared(), SMALL_EPS);
    }

    @Test
    public void noCorrelationPlaintext() {

        List<DeviceDataAsset> assets = List.of(
                plainXY(1, 5),
                plainXY(2, 2),
                plainXY(3, 8),
                plainXY(4, 1));

        QueryResult.LinearRegressionResult r = exec(assets, "production_date", "usage_hours", null);

        Assertions.assertTrue(r.getRSquared() < 0.5, "expect weak/no correlation");
    }

    private DeviceDataAsset buildAsset(long x,
            long y,
            String keyVersion,
            boolean encX,
            boolean encY) {

        Map<String, Object> v = new HashMap<>();

        if (encX)
            v.put("production_date",
                    TestEncryptionService.encryptLong(x, keyVersion));
        else
            v.put("production_date",
                    Timestamp.newBuilder().setSeconds(x).build());

        if (encY)
            v.put("usage_hours",
                    TestEncryptionService.encryptLong(y, keyVersion));
        else
            v.put("usage_hours", (int) y);

        return generator.generateAsset(v, keyVersion);
    }

    @Test
    public void encryptedDataWithoutService() {
        List<DeviceDataAsset> assets = List.of(
                buildAsset(1, 3, "v1", true, true),
                buildAsset(2, 5, "v1", true, true));

        Assertions.assertThrows(NullPointerException.class,
                () -> exec(assets, "production_date", "usage_hours", null));
    }

    @Test
    public void paillier() {
        Set<String> versions = Set.of("paillier-v1");
        TestEncryptionService enc = new TestEncryptionService(true, true,
                versions, "paillier-v1");

        List<DeviceDataAsset> assets = new ArrayList<>();
        for (int x = 1; x <= 5; x++) {
            assets.add(buildAsset(x, 3 * x + 2, "paillier-v1", true, true));
        }

        QueryResult.LinearRegressionResult r = exec(assets, "production_date", "usage_hours", enc);

        double expectedSlope = 3.0 * SECONDS_PER_DAY;
        Assertions.assertEquals(expectedSlope, r.getSlope(), SLOPE_EPS);
        Assertions.assertEquals(2.0, r.getIntercept(), SMALL_EPS);
        Assertions.assertEquals(1.0, r.getRSquared(), SMALL_EPS);
    }

    @Test
    public void mixedPlainAndEncrypted() {
        Set<String> versions = Set.of("paillier-v1");
        TestEncryptionService enc = new TestEncryptionService(true, true,
                versions, "paillier-v1");

        List<DeviceDataAsset> assets = List.of(
                plainXY(1, 3), // plain
                buildAsset(2, 5, "paillier-v1", true, true), // enc
                plainXY(3, 7), // plain
                buildAsset(4, 9, "paillier-v1", true, true)); // enc

        QueryResult.LinearRegressionResult r = exec(assets, "production_date", "usage_hours", enc);

        double expectedSlope = 2.0 * SECONDS_PER_DAY;
        Assertions.assertEquals(expectedSlope, r.getSlope(), SLOPE_EPS);
        Assertions.assertEquals(1.0, r.getIntercept(), SMALL_EPS);
        Assertions.assertTrue(r.getRSquared() > 0.99);
    }

    @Test
    public void weightedAverageAcrossVersions() {
        List<DeviceDataAsset> assets = new ArrayList<>();
        for (int x = 1; x <= 3; x++) {
            assets.add(plainXY(x, x)); // v1
            assets.add(buildAsset(x, 3 * x + 2, "v2", false, false)); // v2
        }

        QueryResult.LinearRegressionResult r = exec(assets, "production_date", "usage_hours", null);

        double expectedSlope = 2.0 * SECONDS_PER_DAY;
        Assertions.assertEquals(expectedSlope, r.getSlope(), SLOPE_EPS);
        Assertions.assertEquals(1.0, r.getIntercept(), SMALL_EPS);
        Assertions.assertTrue(r.getRSquared() > 0.80);
    }

    @Test
    public void largeDatasetPlaintext() {
        List<DeviceDataAsset> assets = new ArrayList<>();
        for (int x = 1; x <= 1_000; x++) {
            assets.add(plainXY(x, 4 * x + 3));
        }

        QueryResult.LinearRegressionResult r = exec(assets, "production_date", "usage_hours", null);

        double expectedSlope = 4.0 * SECONDS_PER_DAY;
        Assertions.assertEquals(expectedSlope, r.getSlope(), SLOPE_EPS);
        Assertions.assertEquals(3.0, r.getIntercept(), SMALL_EPS);
        Assertions.assertEquals(1.0, r.getRSquared(), 1e-5);
    }

    @Test
    public void homomorphicAdditionOnly() {
        Set<String> versions = Set.of("paillier-add");
        TestEncryptionService enc = new TestEncryptionService(true,
                false,
                versions, "paillier-add");

        List<DeviceDataAsset> assets = new ArrayList<>();
        for (int x = 1; x <= 4; x++) {
            assets.add(buildAsset(x, 2 * x + 1, "paillier-add", true, true));
        }

        QueryResult.LinearRegressionResult r = exec(assets, "production_date", "usage_hours", enc);

        double expectedSlope = 2.0 * SECONDS_PER_DAY;
        Assertions.assertEquals(expectedSlope, r.getSlope(), SLOPE_EPS);
        Assertions.assertEquals(1.0, r.getIntercept(), SMALL_EPS);
        Assertions.assertEquals(1.0, r.getRSquared(), SMALL_EPS);
    }

    @Test
    public void heavilyUnequalVersionWeighting() {
        List<DeviceDataAsset> assets = new ArrayList<>();

        for (int x = 1; x <= 10; x++)
            assets.add(buildAsset(x, 2 * x + 1, "vA", false, false));

        for (int x = 1; x <= 2; x++)
            assets.add(buildAsset(x, -3 * x + 5, "vB", false, false));

        QueryResult.LinearRegressionResult r = exec(assets, "production_date", "usage_hours", null);

        double expectedSlope = (14.0 / 12.0) * SECONDS_PER_DAY;
        
        Assertions.assertEquals(expectedSlope, r.getSlope(), SLOPE_EPS);
        Assertions.assertEquals(1.667, r.getIntercept(), 0.02);
        Assertions.assertTrue(r.getRSquared() > 0.70);
    }

    @Test
    public void versionWithSinglePointIgnored() {
        List<DeviceDataAsset> assets = new ArrayList<>();
        for (int x = 1; x <= 5; x++)
            assets.add(buildAsset(x, 2 * x, "v1", false, false));

        assets.add(buildAsset(10, 1234, "v2", false, false));

        QueryResult.LinearRegressionResult r = exec(assets, "production_date", "usage_hours", null);

        double expectedSlope = 2.0 * SECONDS_PER_DAY;
        Assertions.assertEquals(expectedSlope, r.getSlope(), SLOPE_EPS);
        Assertions.assertEquals(0.0, r.getIntercept(), SMALL_EPS);
    }

    @Test
    public void negativeSlopePlaintext() {
        List<DeviceDataAsset> assets = new ArrayList<>();
        for (int x = 1; x <= 5; x++)
            assets.add(plainXY(x, -2 * x + 10));

        QueryResult.LinearRegressionResult r = exec(assets, "production_date", "usage_hours", null);

        double expectedSlope = -2.0 * SECONDS_PER_DAY;
        Assertions.assertEquals(expectedSlope, r.getSlope(), SLOPE_EPS);
        Assertions.assertEquals(10.0, r.getIntercept(), SMALL_EPS);
        Assertions.assertEquals(1.0, r.getRSquared(), SMALL_EPS);
    }
}