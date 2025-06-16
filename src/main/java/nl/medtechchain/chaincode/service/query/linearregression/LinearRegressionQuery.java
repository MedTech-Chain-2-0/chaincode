package nl.medtechchain.chaincode.service.query.linearregression;

import nl.medtechchain.chaincode.service.query.QueryProcessor;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;
import java.util.*;
import java.util.logging.Logger;

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

        //group by version
        Map<String, List<DeviceDataAsset>> versionGroups = groupByVersion(assets);
        
        // store by version
        List<RegressionResult> versionResults = new ArrayList<>();
        int total_points = 0;

        for (Map.Entry<String, List<DeviceDataAsset>> entry : versionGroups.entrySet()) {
            String version = entry.getKey();
            List<DeviceDataAsset> versionAssets = entry.getValue();
            
            if (versionAssets.size() < 2) {
                logger.fine("Skipping version " + version + " with insufficient points: " + versionAssets.size());
                continue;
            }

            logger.fine("Processing regression for " + versionAssets.size() + " assets with version: " + version);
            RegressionResult result = processVersionGroup(versionAssets, query.getTargetField(), version);
            if (result != null) {
                versionResults.add(result);
                total_points += versionAssets.size();
            }
        }

        if (versionResults.isEmpty()) {
            return QueryResult.newBuilder()
                    .setLinearRegressionResult(QueryResult.LinearRegressionResult.newBuilder()
                            .setSlope(0)
                            .setIntercept(0)
                            .setRSquared(0)
                            .build())
                    .build();
        }

        // Calculate weighted average of results based on number of points in each version
        double totalWeightedSlope = 0;
        double totalWeightedIntercept = 0;
        double totalWeightedRSquared = 0;

        for (RegressionResult result : versionResults) {
            double weight = (double) result.point_count / total_points;
            totalWeightedSlope += result.slope * weight;
            totalWeightedIntercept += result.intercept * weight;
            totalWeightedRSquared += result.rSquared * weight;
        }

        return QueryResult.newBuilder()
                .setLinearRegressionResult(QueryResult.LinearRegressionResult.newBuilder()
                        .setSlope(totalWeightedSlope)
                        .setIntercept(totalWeightedIntercept)
                        .setRSquared(totalWeightedRSquared)
                        .build())
                .build();
    }

    private RegressionResult processVersionGroup(List<DeviceDataAsset> assets, String target_field, String version) {
        int n = assets.size();
        double[] xValues = new double[n];
        double[] yValues = new double[n];
        int validPoints = 0;
        List<String> encryptedYValues = new ArrayList<>();
        double plainYSum = 0;
        int plainCount = 0;
        for (int i = 0; i < n; i++) {
            DeviceDataAsset asset = assets.get(i);
            xValues[i] = asset.getTimestamp().getSeconds();
            try {
                var fieldValue = getFieldValue(asset, target_field);
                if (fieldValue.isEncrypted()) {
                    if (encryptionService == null) {
                        throw new IllegalStateException("Found encrypted data but no encryption service configured");
                    }
                    if (encryptionService.isHomomorphic()) {
                        encryptedYValues.add(fieldValue.getEncryptedValue());
                    } else {
                        yValues[i] = encryptionService.decryptLong(fieldValue.getEncryptedValue(), version);
                        validPoints++;
                    }
                } else {
                    yValues[i] = fieldValue.getPlainValue();
                    plainYSum += yValues[i];
                    plainCount++;
                    validPoints++;
                }
            } catch (Exception e) {
                logger.warning("Error processing data point: " + e.getMessage());
                continue;
            }
        }
        if (!encryptedYValues.isEmpty()) {
            String encryptedSum;
            if (encryptedYValues.size() == 1) {
                encryptedSum = encryptedYValues.get(0);
            } else {
                encryptedSum = encryptionService.homomorphicAdd(encryptedYValues, version);
            }
            double decryptedSum = encryptionService.decryptLong(encryptedSum, version);
            plainYSum += decryptedSum;
            plainCount += encryptedYValues.size();
            validPoints += encryptedYValues.size();
        }
        if (validPoints < 2) {
            return null;
        }
        double meanX = Arrays.stream(xValues).average().orElse(0);
        double meanY = plainYSum / plainCount;
        double numerator = 0;
        double denominator = 0;
        for (int i = 0; i < n; i++) {
            double xDiff = xValues[i] - meanX;
            double yDiff = yValues[i] - meanY;
            numerator += xDiff * yDiff;
            denominator += xDiff * xDiff;
        }
        double slope = denominator == 0 ? 0 : numerator / denominator;
        double intercept = meanY - slope * meanX;
        double ssTotal = 0;
        double ssResidual = 0;
        for (int i = 0; i < n; i++) {
            double predictedY = slope * xValues[i] + intercept;
            ssTotal += Math.pow(yValues[i] - meanY, 2);
            ssResidual += Math.pow(yValues[i] - predictedY, 2);
        }
        double rSquared = ssTotal == 0 ? 0 : 1 - (ssResidual / ssTotal);
        return new RegressionResult(slope, intercept, rSquared, validPoints);
    }

    private static class RegressionResult {
        final double slope;
        final double intercept;
        final double rSquared;
        final int point_count;
        RegressionResult(double slope, double intercept, double rSquared, int point_count) {
            this.slope = slope;
            this.intercept = intercept;
            this.rSquared = rSquared;
            this.point_count = point_count;
        }
    }

    private static class FieldValue {
        private final boolean isEncrypted;
        private final String encryptedValue;
        private final double plainValue;

        private FieldValue(boolean isEncrypted, String encryptedValue, double plainValue) {
            this.isEncrypted = isEncrypted;
            this.encryptedValue = encryptedValue;
            this.plainValue = plainValue;
        }

        static FieldValue encrypted(String value) {
            return new FieldValue(true, value, 0);
        }

        static FieldValue plain(double value) {
            return new FieldValue(false, null, value);
        }

        boolean isEncrypted() {
            return isEncrypted;
        }

        String getEncryptedValue() {
            return encryptedValue;
        }

        double getPlainValue() {
            return plainValue;
        }
    }

    private FieldValue getFieldValue(DeviceDataAsset asset, String fieldName) {
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
                    return FieldValue.plain(intField.getPlain());
                } else if (intField.getFieldCase() == DeviceDataAsset.IntegerField.FieldCase.ENCRYPTED) {
                    return FieldValue.encrypted(intField.getEncrypted());
                } else {
                    throw new IllegalArgumentException("Field value not set: " + fieldName);
                }
            case "devicedata.DeviceDataAsset.TimestampField":
                var timestampField = (DeviceDataAsset.TimestampField) field;
                if (timestampField.getFieldCase() == DeviceDataAsset.TimestampField.FieldCase.PLAIN) {
                    long seconds = timestampField.getPlain().getSeconds();
                    if (seconds <= 0) {
                        throw new IllegalArgumentException("Invalid timestamp value: " + seconds);
                    }
                    return FieldValue.plain(seconds);
                } else if (timestampField.getFieldCase() == DeviceDataAsset.TimestampField.FieldCase.ENCRYPTED) {
                    return FieldValue.encrypted(timestampField.getEncrypted());
                } else {
                    throw new IllegalArgumentException("Field value not set: " + fieldName);
                }
            default:
                throw new IllegalArgumentException("Unsupported field type for linear regression: " + fieldName);
        }
    }
} 