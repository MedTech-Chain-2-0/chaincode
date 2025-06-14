package nl.medtechchain.chaincode.service.query.linearregression;

import nl.medtechchain.chaincode.service.query.QueryProcessor;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;

import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class LinearRegressionQuery extends QueryProcessor {
    private static final Logger logger = Logger.getLogger(LinearRegressionQuery.class.getName());

    public LinearRegressionQuery(PlatformConfig platformConfig) {
        super(platformConfig);
    }

    @Override
    public QueryResult process(Query query, List<DeviceDataAsset> assets) {
        if (assets.isEmpty()) {
            return QueryResult.newBuilder()
                .setLinearRegressionResult(QueryResult.LinearRegressionResult.newBuilder()
                    .setSlope(0.0)
                    .setIntercept(0.0)
                    .setRSquared(0.0)
                    .build())
                .build();
        }

        Map<String, List<DeviceDataAsset>> versionGroups = groupByVersion(assets);
        
        double totalSlope = 0.0;
        double totalIntercept = 0.0;
        double totalRSquared = 0.0;
        int validGroups = 0;

        for (Map.Entry<String, List<DeviceDataAsset>> entry : versionGroups.entrySet()) {
            String version = entry.getKey();
            List<DeviceDataAsset> versionAssets = entry.getValue();
            
            logger.fine("Processing linear regression for " + versionAssets.size() + " assets with version: " + version);
            
            QueryResult versionResult = processVersionGroup(query, versionAssets, version);
            if (versionResult.hasLinearRegressionResult()) {
                QueryResult.LinearRegressionResult versionRegression = versionResult.getLinearRegressionResult();
                totalSlope += versionRegression.getSlope();
                totalIntercept += versionRegression.getIntercept();
                totalRSquared += versionRegression.getRSquared();
                validGroups++;
            }
        }

        if (validGroups == 0) {
            logger.warning("No valid version groups found for linear regression");
            return QueryResult.newBuilder()
                .setLinearRegressionResult(QueryResult.LinearRegressionResult.newBuilder()
                    .setSlope(0.0)
                    .setIntercept(0.0)
                    .setRSquared(0.0)
                    .build())
                .build();
        }

        double avgSlope = totalSlope / validGroups;
        double avgIntercept = totalIntercept / validGroups;
        double avgRSquared = totalRSquared / validGroups;

        logger.info("Average linear regression across all versions - Slope: " + avgSlope + 
                   ", Intercept: " + avgIntercept + ", R-squared: " + avgRSquared);

        return QueryResult.newBuilder()
            .setLinearRegressionResult(QueryResult.LinearRegressionResult.newBuilder()
                .setSlope(avgSlope)
                .setIntercept(avgIntercept)
                .setRSquared(avgRSquared)
                .build())
            .build();
    }

    private QueryResult processVersionGroup(Query query, List<DeviceDataAsset> assets, String version) {
        if (assets.size() < 2) {
            logger.fine("Skipping version " + version + " - insufficient data points");
            return QueryResult.newBuilder()
                .setLinearRegressionResult(QueryResult.LinearRegressionResult.newBuilder()
                    .setSlope(0.0)
                    .setIntercept(0.0)
                    .setRSquared(0.0)
                    .build())
                .build();
        }

        // Extract x and y values from the assets in this group
        double[] xValues = new double[assets.size()];
        double[] yValues = new double[assets.size()];
        
        for (int i = 0; i < assets.size(); i++) {
            DeviceDataAsset asset = assets.get(i);
            xValues[i] = asset.getTimestamp().getSeconds();
            yValues[i] = getFieldValue(asset, query.getTargetField());
        }

        double[] regression = calculateLinearRegression(xValues, yValues);
        logger.fine("Linear regression for version " + version + 
                   " - Slope: " + regression[0] + 
                   ", Intercept: " + regression[1] + 
                   ", R-squared: " + regression[2]);

        return QueryResult.newBuilder()
            .setLinearRegressionResult(QueryResult.LinearRegressionResult.newBuilder()
                .setSlope(regression[0])
                .setIntercept(regression[1])
                .setRSquared(regression[2])
                .build())
            .build();
    }

    private double getFieldValue(DeviceDataAsset asset, String fieldName) {
        var descriptor = getFieldDescriptor(fieldName);
        if (descriptor == null) {
            throw new IllegalArgumentException("Unknown field: " + fieldName);
        }

        var field = asset.getDeviceData().getField(descriptor);
        if (field == null) {
            throw new IllegalArgumentException("Field not set: " + fieldName);
        }

        switch (descriptor.getMessageType().getFullName()) {
            case "devicedata.DeviceDataAsset.IntegerField":
                var intField = (DeviceDataAsset.IntegerField) field;
                if (intField.getFieldCase() == DeviceDataAsset.IntegerField.FieldCase.PLAIN) {
                    return intField.getPlain();
                } else {
                    throw new IllegalArgumentException("Encrypted integer fields not supported for linear regression");
                }
            case "devicedata.DeviceDataAsset.TimestampField":
                var timestampField = (DeviceDataAsset.TimestampField) field;
                if (timestampField.getFieldCase() == DeviceDataAsset.TimestampField.FieldCase.PLAIN) {
                    return timestampField.getPlain().getSeconds();
                } else {
                    throw new IllegalArgumentException("Encrypted timestamp fields not supported for linear regression");
                }
            default:
                throw new IllegalArgumentException("Unsupported field type for linear regression: " + fieldName);
        }
    }

    private double[] calculateLinearRegression(double[] x, double[] y) {
        int n = x.length;
        double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0, sumY2 = 0;

        for (int i = 0; i < n; i++) {
            sumX += x[i];
            sumY += y[i];
            sumXY += x[i] * y[i];
            sumX2 += x[i] * x[i];
            sumY2 += y[i] * y[i];
        }

        double slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
        double intercept = (sumY - slope * sumX) / n;


        double meanY = sumY / n;
        double ssTotal = 0, ssResidual = 0;
        for (int i = 0; i < n; i++) {
            double predicted = slope * x[i] + intercept;
            ssTotal += Math.pow(y[i] - meanY, 2);
            ssResidual += Math.pow(y[i] - predicted, 2);
        }
        double rSquared = 1 - (ssResidual / ssTotal);

        return new double[]{slope, intercept, rSquared};
    }
} 