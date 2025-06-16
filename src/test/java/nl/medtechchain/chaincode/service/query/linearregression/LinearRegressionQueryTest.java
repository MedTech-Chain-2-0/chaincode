package nl.medtechchain.chaincode.service.query.linearregression;

import com.google.protobuf.Timestamp;
import nl.medtechchain.chaincode.service.query.TestEncryptionService;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class LinearRegressionQueryTest {

    private LinearRegressionQuery linearRegressionQuery;
    private PlatformConfig platformConfig;
    private TestEncryptionService encryptionService;

    @BeforeEach
    void setUp() {
        platformConfig = PlatformConfig.getDefaultInstance();
        linearRegressionQuery = new LinearRegressionQuery(platformConfig);
        // Initialize encryption service with no encryption for plaintext tests
        encryptionService = new TestEncryptionService(false, false, Set.of(), null);
        try {
            var encryptionServiceField = linearRegressionQuery.getClass().getSuperclass().getDeclaredField("encryptionService");
            encryptionServiceField.setAccessible(true);
            encryptionServiceField.set(linearRegressionQuery, encryptionService);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject test encryption service", e);
        }
    }

    @Test
    void testEmptyAssetsList() {
        Query query = Query.newBuilder()
                .setQueryType(Query.QueryType.LINEAR_REGRESSION)
                .setTargetField("usage_hours")
                .build();

        QueryResult result = linearRegressionQuery.process(query, new ArrayList<>());

        assertNotNull(result);
        assertTrue(result.hasLinearRegressionResult());
        assertEquals(0.0, result.getLinearRegressionResult().getSlope());
        assertEquals(0.0, result.getLinearRegressionResult().getIntercept());
        assertEquals(0.0, result.getLinearRegressionResult().getRSquared());
    }

    @Test
    void testSingleAsset() {
        List<DeviceDataAsset> assets = new ArrayList<>();
        assets.add(createDeviceDataAsset(1000, 10)); // timestamp, usage hours

        Query query = Query.newBuilder()
                .setQueryType(Query.QueryType.LINEAR_REGRESSION)
                .setTargetField("usage_hours")
                .build();

        QueryResult result = linearRegressionQuery.process(query, assets);

        assertNotNull(result);
        assertTrue(result.hasLinearRegressionResult());
        assertEquals(0.0, result.getLinearRegressionResult().getSlope());
        assertEquals(0.0, result.getLinearRegressionResult().getIntercept());
        assertEquals(0.0, result.getLinearRegressionResult().getRSquared());
    }

    @Test
    void testPerfectLinearCorrelation() {
        List<DeviceDataAsset> assets = new ArrayList<>();
        // Create assets with perfect linear correlation (y = 2x + 1)
        // Using timestamps as x values: 1000, 2000, 3000, 4000
        // For y = 2x + 1, we need to scale the y values accordingly
        assets.add(createDeviceDataAsset(1000, 2001));  
        assets.add(createDeviceDataAsset(2000, 4001));  
        assets.add(createDeviceDataAsset(3000, 6001));  
        assets.add(createDeviceDataAsset(4000, 8001));  

        Query query = Query.newBuilder()
                .setQueryType(Query.QueryType.LINEAR_REGRESSION)
                .setTargetField("usage_hours")
                .build();

        QueryResult result = linearRegressionQuery.process(query, assets);

        assertNotNull(result);
        assertTrue(result.hasLinearRegressionResult());
        assertEquals(2.0, result.getLinearRegressionResult().getSlope(), 0.001);
        assertEquals(1.0, result.getLinearRegressionResult().getIntercept(), 0.001);
        assertEquals(1.0, result.getLinearRegressionResult().getRSquared(), 0.001);
    }

    @Test
    void testNoCorrelation() {
        List<DeviceDataAsset> assets = new ArrayList<>();

        assets.add(createDeviceDataAsset(1000, 5));
        assets.add(createDeviceDataAsset(2000, 2));
        assets.add(createDeviceDataAsset(3000, 8));
        assets.add(createDeviceDataAsset(4000, 3));

        Query query = Query.newBuilder()
                .setQueryType(Query.QueryType.LINEAR_REGRESSION)
                .setTargetField("usage_hours")
                .build();

        QueryResult result = linearRegressionQuery.process(query, assets);

        assertNotNull(result);
        assertTrue(result.hasLinearRegressionResult());
        assertTrue(result.getLinearRegressionResult().getRSquared() < 0.5);
    }

    @Test
    void testMultipleVersions() {
        List<DeviceDataAsset> assets = new ArrayList<>();

        assets.add(createDeviceDataAsset(1000, 3, "v1"));
        assets.add(createDeviceDataAsset(2000, 5, "v1"));
        assets.add(createDeviceDataAsset(1000, 4, "v2"));
        assets.add(createDeviceDataAsset(2000, 6, "v2"));

        Query query = Query.newBuilder()
                .setQueryType(Query.QueryType.LINEAR_REGRESSION)
                .setTargetField("usage_hours")
                .build();

        QueryResult result = linearRegressionQuery.process(query, assets);

        assertNotNull(result);
        assertTrue(result.hasLinearRegressionResult());
        assertTrue(result.getLinearRegressionResult().getRSquared() > 0.5);
    }

    private DeviceDataAsset createDeviceDataAsset(long timestamp, int usageHours) {
        return createDeviceDataAsset(timestamp, usageHours, "v1");
    }

    private DeviceDataAsset createDeviceDataAsset(long timestamp, int usageHours, String keyVersion) {
        return DeviceDataAsset.newBuilder()
                .setTimestamp(Timestamp.newBuilder().setSeconds(timestamp).build())
                .setDeviceData(DeviceDataAsset.DeviceData.newBuilder()
                        .setUsageHours(DeviceDataAsset.IntegerField.newBuilder()
                                .setPlain(usageHours)
                                .build())
                        .build())
                .setKeyVersion(keyVersion)
                .build();
    }
} 