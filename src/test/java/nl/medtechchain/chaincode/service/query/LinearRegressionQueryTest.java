package nl.medtechchain.chaincode.service.query;

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
    private TestDataGenerator generator;
    private PlatformConfig testConfig;
    
    @BeforeEach
    public void setUp() {
        generator = new TestDataGenerator(42); 
        testConfig = PlatformConfig.newBuilder().build();
    }

    private Query buildLinearRegressionQuery(String targetField) {
        return Query.newBuilder()
                .setQueryType(Query.QueryType.LINEAR_REGRESSION)
                .setTargetField(targetField)
                .build();
    }

    private QueryResult.LinearRegressionResult executeLinearRegression(List<DeviceDataAsset> assets, String targetField) {
        LinearRegressionQuery query = new LinearRegressionQuery(testConfig);
        Query queryObj = buildLinearRegressionQuery(targetField);
        QueryResult result = query.process(queryObj, assets);
        return result.getLinearRegressionResult();
    }

    @Test
    public void testEmptyAssetList() {
        List<DeviceDataAsset> assets = Collections.emptyList();
        QueryResult.LinearRegressionResult result = executeLinearRegression(assets, "usage_hours");
        
        Assertions.assertEquals(0.0, result.getSlope(), "Slope should be 0 for empty list");
        Assertions.assertEquals(0.0, result.getIntercept(), "Intercept should be 0 for empty list");
        Assertions.assertEquals(0.0, result.getRSquared(), "R-squared should be 0 for empty list");
    }

    @Test
    public void testPerfectLinearCorrelation() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        // Create a perfect linear relationship: y = 2x + 1
        usageHours.put(1, 1);  // x=1, y=3
        usageHours.put(2, 1);  // x=2, y=5
        usageHours.put(3, 1);  // x=3, y=7
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 3);
        QueryResult.LinearRegressionResult result = executeLinearRegression(assets, "usage_hours");
        
        Assertions.assertEquals(2.0, result.getSlope(), 0.001, "Slope should be 2");
        Assertions.assertEquals(1.0, result.getIntercept(), 0.001, "Intercept should be 1");
        Assertions.assertEquals(1.0, result.getRSquared(), 0.001, "R-squared should be 1 for perfect correlation");
    }

    @Test
    public void testNoCorrelation() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        // Create data with no correlation
        usageHours.put(1, 1);  // x=1, y=1
        usageHours.put(2, 1);  // x=2, y=1
        usageHours.put(3, 1);  // x=3, y=1
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 3);
        QueryResult.LinearRegressionResult result = executeLinearRegression(assets, "usage_hours");
        
        Assertions.assertEquals(0.0, result.getSlope(), 0.001, "Slope should be 0 for no correlation");
        Assertions.assertEquals(1.0, result.getIntercept(), 0.001, "Intercept should be 1");
        Assertions.assertEquals(0.0, result.getRSquared(), 0.001, "R-squared should be 0 for no correlation");
    }

    @Test
    public void testNegativeCorrelation() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        // Create a negative linear relationship: y = -2x + 7
        usageHours.put(1, 1);  // x=1, y=5
        usageHours.put(2, 1);  // x=2, y=3
        usageHours.put(3, 1);  // x=3, y=1
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 3);
        QueryResult.LinearRegressionResult result = executeLinearRegression(assets, "usage_hours");
        
        Assertions.assertEquals(-2.0, result.getSlope(), 0.001, "Slope should be -2");
        Assertions.assertEquals(7.0, result.getIntercept(), 0.001, "Intercept should be 7");
        Assertions.assertEquals(1.0, result.getRSquared(), 0.001, "R-squared should be 1 for perfect negative correlation");
    }

    @Test
    public void testLargeDataset() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        // Create a large dataset with a linear relationship: y = 3x + 2
        for (int i = 1; i <= 10; i++) {
            usageHours.put(i, 1);  // x=i, y=3i+2
        }
        spec.put("usage_hours", usageHours);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 10);
        QueryResult.LinearRegressionResult result = executeLinearRegression(assets, "usage_hours");
        
        Assertions.assertEquals(3.0, result.getSlope(), 0.001, "Slope should be 3");
        Assertions.assertEquals(2.0, result.getIntercept(), 0.001, "Intercept should be 2");
        Assertions.assertEquals(1.0, result.getRSquared(), 0.001, "R-squared should be 1 for perfect correlation");
    }

    @Test
    public void testTimestampField() {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> productionDate = new HashMap<>();
        
        // Create timestamps with a linear relationship
        long baseTime = 1609459200; // 2021-01-01
        for (int i = 0; i < 5; i++) {
            long timestamp = baseTime + (i * 86400); // Add one day each time
            productionDate.put(com.google.protobuf.Timestamp.newBuilder().setSeconds(timestamp).build(), 1);
        }
        spec.put("production_date", productionDate);
        
        List<DeviceDataAsset> assets = generator.generateAssetsWithCounts(spec, 5);
        QueryResult.LinearRegressionResult result = executeLinearRegression(assets, "production_date");
        
        // The slope should be approximately 86400 (seconds per day)
        Assertions.assertEquals(86400.0, result.getSlope(), 0.001, "Slope should be 86400 (seconds per day)");
        Assertions.assertEquals(baseTime, result.getIntercept(), 0.001, "Intercept should be the base timestamp");
        Assertions.assertEquals(1.0, result.getRSquared(), 0.001, "R-squared should be 1 for perfect correlation");
    }
} 