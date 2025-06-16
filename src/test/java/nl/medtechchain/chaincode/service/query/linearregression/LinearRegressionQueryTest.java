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

    /* 
    @Test
    void testLinearRegressionWithDifferentKeys() {
        // Create test data with different keys
        List<DeviceDataAsset> assets = new ArrayList<>();
        
        // Group 1 (key1): (1,2), (2,4)
        assets.add(createDeviceDataAsset(1, 2, "key1"));
        assets.add(createDeviceDataAsset(2, 4, "key1"));
        
        // Group 2 (key2): (3,5)
        assets.add(createDeviceDataAsset(3, 5, "key2"));
        
        // Group 3 (key3): (4,4)
        assets.add(createDeviceDataAsset(4, 4, "key3"));
        
        // Group 4 (key4): (5,6)
        assets.add(createDeviceDataAsset(5, 6, "key4"));

        // Create query
        Query queryRequest = Query.newBuilder()
                .setQueryType(Query.QueryType.LINEAR_REGRESSION)
                .setTargetField("usage_hours")
                .build();

        // Process query
        QueryResult result = linearRegressionQuery.process(queryRequest, assets);
        QueryResult.LinearRegressionResult regressionResult = result.getLinearRegressionResult();

        // Expected values:
        // For key1 group (n=2):
        //   sumX = 3, sumY = 6, sumXY = 10, sumXX = 5, sumYY = 20
        // For key2 group (n=1):
        //   sumX = 3, sumY = 5, sumXY = 15, sumXX = 9, sumYY = 25
        // For key3 group (n=1):
        //   sumX = 4, sumY = 4, sumXY = 16, sumXX = 16, sumYY = 16
        // For key4 group (n=1):
        //   sumX = 5, sumY = 6, sumXY = 30, sumXX = 25, sumYY = 36
        //
        // Total:
        // n = 5
        // sumX = 15
        // sumY = 21
        // sumXY = 73
        // sumXX = 55
        // sumYY = 97
        //
        // slope = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX)
        //       = (5 * 73 - 15 * 21) / (5 * 55 - 15 * 15)
        //       = (365 - 315) / (275 - 225)
        //       = 50 / 50
        //       = 1.0
        //
        // intercept = (sumY - slope * sumX) / n
        //          = (21 - 1.0 * 15) / 5
        //          = 6 / 5
        //          = 1.2

        // Verify results
        assertEquals(1.0, regressionResult.getSlope(), 0.0001, "Slope should be 1.0");
        assertEquals(1.2, regressionResult.getIntercept(), 0.0001, "Intercept should be 1.2");
        assertTrue(regressionResult.getRSquared() > 0.5, "R-squared should be positive");
    }
*/
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