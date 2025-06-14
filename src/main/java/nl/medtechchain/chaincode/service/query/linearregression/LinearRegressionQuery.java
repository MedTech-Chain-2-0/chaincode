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
                            .setSlope(0)
                            .setIntercept(0)
                            .setRSquared(0)
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
            if (versionAssets.size() < 2) continue;
            QueryResult.LinearRegressionResult result = calculateRegression(versionAssets, query.getTargetField());
            totalSlope += result.getSlope();
            totalIntercept += result.getIntercept();
            totalRSquared += result.getRSquared();
            validGroups++;
        }

        if (validGroups == 0) {
            return QueryResult.newBuilder()
                    .setLinearRegressionResult(QueryResult.LinearRegressionResult.newBuilder()
                            .setSlope(0)
                            .setIntercept(0)
                            .setRSquared(0)
                            .build())
                    .build();
        }

        double avgSlope = totalSlope / validGroups;
        double avgIntercept = totalIntercept / validGroups;
        double avgRSquared = totalRSquared / validGroups;

        return QueryResult.newBuilder()
                .setLinearRegressionResult(QueryResult.LinearRegressionResult.newBuilder()
                        .setSlope(avgSlope)
                        .setIntercept(avgIntercept)
                        .setRSquared(avgRSquared)
                        .build())
                .build();
    }

    private QueryResult.LinearRegressionResult calculateRegression(List<DeviceDataAsset> assets, String targetField) {
        int n = assets.size();
        double[] xValues = new double[n];
        double[] yValues = new double[n];
        for (int i = 0; i < n; i++) {
            DeviceDataAsset asset = assets.get(i);
            xValues[i] = asset.getTimestamp().getSeconds();
            yValues[i] = getFieldValue(asset, targetField);
        }
        double meanX = Arrays.stream(xValues).average().orElse(0);
        double meanY = Arrays.stream(yValues).average().orElse(0);
        double numerator = 0, denominator = 0;
        for (int i = 0; i < n; i++) {
            double xDiff = xValues[i] - meanX;
            double yDiff = yValues[i] - meanY;
            numerator += xDiff * yDiff;
            denominator += xDiff * xDiff;
        }
        double slope = denominator == 0 ? 0 : numerator / denominator;
        double intercept = meanY - slope * meanX;
        double ssTotal = 0, ssResidual = 0;
        for (int i = 0; i < n; i++) {
            double predictedY = slope * xValues[i] + intercept;
            ssTotal += Math.pow(yValues[i] - meanY, 2);
            ssResidual += Math.pow(yValues[i] - predictedY, 2);
        }
        double rSquared = ssTotal == 0 ? 0 : 1 - (ssResidual / ssTotal);
        return QueryResult.LinearRegressionResult.newBuilder()
                .setSlope(slope)
                .setIntercept(intercept)
                .setRSquared(rSquared)
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
                    long seconds = timestampField.getPlain().getSeconds();
                    if (seconds <= 0) {
                        throw new IllegalArgumentException("Invalid timestamp value: " + seconds);
                    }
                    return seconds;
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