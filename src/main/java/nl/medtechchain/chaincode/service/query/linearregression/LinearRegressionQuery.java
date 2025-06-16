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
        if (assets == null || assets.isEmpty()) {
            return createEmptyResult();
        }

        var fieldDescriptor = getFieldDescriptor(query.getTargetField());
        if (fieldDescriptor == null) {
            throw new IllegalArgumentException("Field descriptor is required");
        }

        // Group assets by version
        Map<String, List<DeviceDataAsset>> versionGroups = groupByVersion(assets);

        List<RegressionResult> versionResults = new ArrayList<>();
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
            }
        }

        if (versionResults.isEmpty()) {
            return createEmptyResult();
        }

        // Calculate weighted average of results based on number of points in each version
        double totalPoints = versionResults.stream().mapToInt(RegressionResult::getCount).sum();
        double weightedSlope = 0;
        double weightedIntercept = 0;
        double weightedRSquared = 0;

        for (RegressionResult result : versionResults) {
            double weight = result.getCount() / totalPoints;
            weightedSlope += result.getSlope() * weight;
            weightedIntercept += result.getIntercept() * weight;
            weightedRSquared += result.getRSquared() * weight;
        }

        return QueryResult.newBuilder()
            .setLinearRegressionResult(QueryResult.LinearRegressionResult.newBuilder()
                .setSlope(weightedSlope)
                .setIntercept(weightedIntercept)
                .setRSquared(weightedRSquared)
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

        // Initialize sums for regression calculation
        double sumX = 0;
        double sumY = 0;
        double sumXY = 0;
        double sumX2 = 0;
        double sumY2 = 0;
        int count = 0;

        // Process each asset in the version group
        for (DeviceDataAsset asset : assets) {
            double x = asset.getTimestamp().getSeconds();
            var fieldType = asset.getDeviceData().getField(fieldDescriptor);
            double y = 0;
            boolean validValue = false;

            if (fieldType instanceof DeviceDataAsset.IntegerField) {
                var fieldValue = (DeviceDataAsset.IntegerField) fieldType;
                switch (fieldValue.getFieldCase()) {
                    case PLAIN:
                        y = fieldValue.getPlain();
                        validValue = true;
                        break;
                    case ENCRYPTED:
                        if (encryptionService == null) {
                            throw new IllegalStateException("Found encrypted data but no encryption service configured. " +
                                "Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME to 'paillier' or 'bfv'.");
                        }
                        y = encryptionService.decryptLong(fieldValue.getEncrypted(), version);
                        validValue = true;
                        break;
                    case FIELD_NOT_SET:
                        logger.fine("Skipping asset with no value for field: " + fieldDescriptor.getName());
                        break;
                }
            } else if (fieldType instanceof DeviceDataAsset.TimestampField) {
                var fieldValue = (DeviceDataAsset.TimestampField) fieldType;
                switch (fieldValue.getFieldCase()) {
                    case PLAIN:
                        y = fieldValue.getPlain().getSeconds();
                        validValue = true;
                        break;
                    case ENCRYPTED:
                        if (encryptionService == null) {
                            throw new IllegalStateException("Found encrypted data but no encryption service configured. " +
                                "Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME to 'paillier' or 'bfv'.");
                        }
                        y = encryptionService.decryptLong(fieldValue.getEncrypted(), version);
                        validValue = true;
                        break;
                    case FIELD_NOT_SET:
                        logger.fine("Skipping asset with no value for field: " + fieldDescriptor.getName());
                        break;
                }
            }

            if (validValue) {
                sumX += x;
                sumY += y;
                sumXY += x * y;
                sumX2 += x * x;
                sumY2 += y * y;
                count++;
            }
        }

        if (count < 2) {
            return null;
        }

        // Calculate slope and intercept using the standard formulas
        double slope = (count * sumXY - sumX * sumY) / (count * sumX2 - sumX * sumX);
        double intercept = (sumY - slope * sumX) / count;

        // Calculate R-squared
        double meanY = sumY / count;
        double ssTotal = sumY2 - 2 * meanY * sumY + count * meanY * meanY;
        double ssResidual = 0;

        // Calculate residuals for R-squared
        for (DeviceDataAsset asset : assets) {
            double x = asset.getTimestamp().getSeconds();
            var fieldType = asset.getDeviceData().getField(fieldDescriptor);
            double y = 0;
            boolean validValue = false;

            if (fieldType instanceof DeviceDataAsset.IntegerField) {
                var fieldValue = (DeviceDataAsset.IntegerField) fieldType;
                switch (fieldValue.getFieldCase()) {
                    case PLAIN:
                        y = fieldValue.getPlain();
                        validValue = true;
                        break;
                    case ENCRYPTED:
                        if (encryptionService != null) {
                            y = encryptionService.decryptLong(fieldValue.getEncrypted(), version);
                            validValue = true;
                        }
                        break;
                }
            } else if (fieldType instanceof DeviceDataAsset.TimestampField) {
                var fieldValue = (DeviceDataAsset.TimestampField) fieldType;
                switch (fieldValue.getFieldCase()) {
                    case PLAIN:
                        y = fieldValue.getPlain().getSeconds();
                        validValue = true;
                        break;
                    case ENCRYPTED:
                        if (encryptionService != null) {
                            y = encryptionService.decryptLong(fieldValue.getEncrypted(), version);
                            validValue = true;
                        }
                        break;
                }
            }

            if (validValue) {
                double predictedY = slope * x + intercept;
                ssResidual += (y - predictedY) * (y - predictedY);
            }
        }

        double rSquared = 1.0 - (ssResidual / ssTotal);
        logger.fine("Regression results for version " + version + ": slope=" + slope + 
                   ", intercept=" + intercept + ", rSquared=" + rSquared + ", count=" + count);

        return new RegressionResult(slope, intercept, rSquared, count);
    }

    private static class RegressionResult {
        private final double slope;
        private final double intercept;
        private final double rSquared;
        private final int count;

        public RegressionResult(double slope, double intercept, double rSquared, int count) {
            this.slope = slope;
            this.intercept = intercept;
            this.rSquared = rSquared;
            this.count = count;
        }

        public double getSlope() {
            return slope;
        }

        public double getIntercept() {
            return intercept;
        }

        public double getRSquared() {
            return rSquared;
        }

        public int getCount() {
            return count;
        }
    }
} 