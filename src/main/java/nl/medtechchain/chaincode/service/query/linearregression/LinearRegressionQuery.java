package nl.medtechchain.chaincode.service.query.linearregression;

import nl.medtechchain.chaincode.service.query.QueryProcessor;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;
import com.google.protobuf.Descriptors;
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
            return createEmptyResult();
        }

        var fieldDescriptor = getFieldDescriptor(query.getTargetField());
        Map<String, List<DeviceDataAsset>> versionGroups = groupByVersion(assets);
        
        List<RegressionResult> versionResults = new ArrayList<>();
        int totalPoints = 0;

        for (Map.Entry<String, List<DeviceDataAsset>> entry : versionGroups.entrySet()) {
            String version = entry.getKey();
            List<DeviceDataAsset> versionAssets = entry.getValue();
            
            if (versionAssets.size() < 2) {
                logger.fine("Skipping version " + version + " with insufficient points: " + versionAssets.size());
                continue;
            }

            logger.fine("Processing regression for " + versionAssets.size() + " assets with version: " + version);
            RegressionResult result = processVersionGroup(versionAssets, fieldDescriptor, version);
            if (result != null) {
                versionResults.add(result);
                totalPoints += versionAssets.size();
            }
        }

        if (versionResults.isEmpty()) {
            return createEmptyResult();
        }

        // Calculate weighted average of results based on number of points in each version
        double totalWeightedSlope = 0;
        double totalWeightedIntercept = 0;
        double totalWeightedRSquared = 0;

        for (RegressionResult result : versionResults) {
            double weight = (double) result.pointCount / totalPoints;
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

    private QueryResult createEmptyResult() {
        return QueryResult.newBuilder()
                .setLinearRegressionResult(QueryResult.LinearRegressionResult.newBuilder()
                        .setSlope(0)
                        .setIntercept(0)
                        .setRSquared(0)
                        .build())
                .build();
    }

    private static class FieldValueResult {
        final double plainYSum;
        final int plainCount;
        final List<String> encryptedYValues;

        FieldValueResult(double plainYSum, int plainCount, List<String> encryptedYValues) {
            this.plainYSum = plainYSum;
            this.plainCount = plainCount;
            this.encryptedYValues = encryptedYValues;
        }
    }

    private FieldValueResult processFieldValue(DeviceDataAsset.IntegerField fieldValue, 
                                            double plainYSum, 
                                            int plainCount, 
                                            List<String> encryptedYValues, 
                                            String version) {
        switch (fieldValue.getFieldCase()) {
            case PLAIN:
                return new FieldValueResult(plainYSum + fieldValue.getPlain(), plainCount + 1, encryptedYValues);
            case ENCRYPTED:
                if (encryptionService == null) {
                    throw new IllegalStateException("Found encrypted data but no encryption service configured. " +
                        "Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME to 'paillier' or 'bfv'.");
                }
                if (encryptionService.isHomomorphic()) {
                    encryptedYValues.add(fieldValue.getEncrypted());
                    return new FieldValueResult(plainYSum, plainCount, encryptedYValues);
                } else {
                    return new FieldValueResult(
                        plainYSum + encryptionService.decryptLong(fieldValue.getEncrypted(), version),
                        plainCount + 1,
                        encryptedYValues
                    );
                }
            case FIELD_NOT_SET:
                logger.fine("Skipping asset with no value for field");
                return new FieldValueResult(plainYSum, plainCount, encryptedYValues);
        }
        return new FieldValueResult(plainYSum, plainCount, encryptedYValues);
    }

    private FieldValueResult processFieldValue(DeviceDataAsset.TimestampField fieldValue, 
                                            double plainYSum, 
                                            int plainCount, 
                                            List<String> encryptedYValues, 
                                            String version) {
        switch (fieldValue.getFieldCase()) {
            case PLAIN:
                return new FieldValueResult(
                    plainYSum + fieldValue.getPlain().getSeconds(),
                    plainCount + 1,
                    encryptedYValues
                );
            case ENCRYPTED:
                if (encryptionService == null) {
                    throw new IllegalStateException("Found encrypted data but no encryption service configured. " +
                        "Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME to 'paillier' or 'bfv'.");
                }
                if (encryptionService.isHomomorphic()) {
                    encryptedYValues.add(fieldValue.getEncrypted());
                    return new FieldValueResult(plainYSum, plainCount, encryptedYValues);
                } else {
                    return new FieldValueResult(
                        plainYSum + encryptionService.decryptLong(fieldValue.getEncrypted(), version),
                        plainCount + 1,
                        encryptedYValues
                    );
                }
            case FIELD_NOT_SET:
                logger.fine("Skipping asset with no value for field");
                return new FieldValueResult(plainYSum, plainCount, encryptedYValues);
        }
        return new FieldValueResult(plainYSum, plainCount, encryptedYValues);
    }

    private RegressionResult processVersionGroup(List<DeviceDataAsset> assets, 
                                               Descriptors.FieldDescriptor fieldDescriptor, 
                                               String version) {
        if (assets.size() < 2) {
            return null;
        }

        // Extract x values (timestamps) and y values
        double[] xValues = new double[assets.size()];
        double[] yValues = new double[assets.size()];
        List<String> encryptedYValues = new ArrayList<>();
        double plainYSum = 0;
        int plainCount = 0;

        for (int i = 0; i < assets.size(); i++) {
            DeviceDataAsset asset = assets.get(i);
            xValues[i] = asset.getTimestamp().getSeconds();

            var fieldType = asset.getDeviceData().getField(fieldDescriptor);
            FieldValueResult result;
            if (fieldType instanceof DeviceDataAsset.IntegerField) {
                result = processFieldValue((DeviceDataAsset.IntegerField) fieldType, plainYSum, plainCount, encryptedYValues, version);
            } else if (fieldType instanceof DeviceDataAsset.TimestampField) {
                result = processFieldValue((DeviceDataAsset.TimestampField) fieldType, plainYSum, plainCount, encryptedYValues, version);
            } else {
                continue;
            }
            plainYSum = result.plainYSum;
            plainCount = result.plainCount;
            encryptedYValues = result.encryptedYValues;
            yValues[i] = getYValue(asset, fieldDescriptor, version);
        }

        // Handle encrypted values if any
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
        }

        if (plainCount < 2) {
            return null;
        }

        // Calculate sums for regression
        double sumX = 0;
        double sumY = 0;
        double sumXY = 0;
        double sumXX = 0;
        double sumYY = 0;

        for (int i = 0; i < assets.size(); i++) {
            double x = xValues[i];
            double y = yValues[i];
            sumX += x;
            sumY += y;
            sumXY += x * y;
            sumXX += x * x;
            sumYY += y * y;
        }

        // Calculate slope and intercept using the correct formulas
        double n = plainCount;
        double denominator = (n * sumXX - sumX * sumX);
        double slope = denominator == 0 ? 0 : (n * sumXY - sumX * sumY) / denominator;
        double intercept = (sumY - slope * sumX) / n;

        // Calculate R-squared
        double meanY = sumY / n;
        double ssTotal = sumYY - (sumY * sumY) / n;
        double ssResidual = 0;
        for (int i = 0; i < assets.size(); i++) {
            double x = xValues[i];
            double y = yValues[i];
            double predictedY = slope * x + intercept;
            ssResidual += (y - predictedY) * (y - predictedY);
        }

        double rSquared = ssTotal == 0 ? 0 : 1 - (ssResidual / ssTotal);
        return new RegressionResult(slope, intercept, rSquared, plainCount);
    }

    private double getYValue(DeviceDataAsset asset, Descriptors.FieldDescriptor fieldDescriptor, String version) {
        var fieldType = asset.getDeviceData().getField(fieldDescriptor);
        if (fieldType instanceof DeviceDataAsset.IntegerField) {
            var fieldValue = (DeviceDataAsset.IntegerField) fieldType;
            switch (fieldValue.getFieldCase()) {
                case PLAIN:
                    return fieldValue.getPlain();
                case ENCRYPTED:
                    return encryptionService.decryptLong(fieldValue.getEncrypted(), version);
                default:
                    return 0;
            }
        } else if (fieldType instanceof DeviceDataAsset.TimestampField) {
            var fieldValue = (DeviceDataAsset.TimestampField) fieldType;
            switch (fieldValue.getFieldCase()) {
                case PLAIN:
                    return fieldValue.getPlain().getSeconds();
                case ENCRYPTED:
                    return encryptionService.decryptLong(fieldValue.getEncrypted(), version);
                default:
                    return 0;
            }
        }
        return 0;
    }

    private static class RegressionResult {
        final double slope;
        final double intercept;
        final double rSquared;
        final int pointCount;

        RegressionResult(double slope, double intercept, double rSquared, int pointCount) {
            this.slope = slope;
            this.intercept = intercept;
            this.rSquared = rSquared;
            this.pointCount = pointCount;
        }
    }
} 