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

        var xFieldDescriptor = getFieldDescriptor(query.getXTargetField());
        var yFieldDescriptor = getFieldDescriptor(query.getYTargetField());

        if (xFieldDescriptor == null || yFieldDescriptor == null) {
            throw new IllegalArgumentException("Field descriptor is null");
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
            RegressionResult result = processVersionGroup(versionAssets, xFieldDescriptor, yFieldDescriptor, version);
            if (result != null) {
                versionResults.add(result);
            }
        }

        if (versionResults.isEmpty()) {
            return createEmptyResult();
        }

        // Calculate weighted average of results based on number of points in each
        // version
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

    private RegressionResult processVersionGroup(
            List<DeviceDataAsset> assets,
            // x field assumed by design to always be a timestamp
            Descriptors.FieldDescriptor xDesc,
            // y field assumed by design to always be an integer
            Descriptors.FieldDescriptor yDesc,
            String version) {

        double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0, sumY2 = 0;
        int count = 0;
        boolean homomorphic = encryptionService != null && encryptionService.isHomomorphic();
        boolean canMultiply = homomorphic && encryptionService.supportsMultiplication();

        // ciphertext buckets – only used when the scheme is homomorphic
        List<String> encXList = new ArrayList<>();
        List<String> encYList = new ArrayList<>();
        List<String> encXYList = new ArrayList<>();
        List<String> encX2List = new ArrayList<>();
        List<String> encY2List = new ArrayList<>();

        for (DeviceDataAsset asset : assets) {
            // reminder: always a timestamp!
            DeviceDataAsset.TimestampField xField = (DeviceDataAsset.TimestampField) asset.getDeviceData()
                    .getField(xDesc);

            Double xPlain = null;
            String xEnc = null;
            if (xField != null)
                switch (xField.getFieldCase()) {
                    case PLAIN:
                        xPlain = (double) xField.getPlain().getSeconds();
                        break;
                    case ENCRYPTED:
                        if (homomorphic)
                            xEnc = xField.getEncrypted();
                        else
                            xPlain = (double) encryptionService.decryptLong(xField.getEncrypted(), version);
                        break;
                    default:
                        logger.fine("Skipping asset with no value for case " + xField.getFieldCase());
                        break;
                }

            DeviceDataAsset.IntegerField yField = (DeviceDataAsset.IntegerField) asset.getDeviceData().getField(yDesc);

            Double yPlain = null;
            String yEnc = null;
            if (yField != null)
                switch (yField.getFieldCase()) {
                    case PLAIN:
                        yPlain = (double) yField.getPlain();
                        break;
                    case ENCRYPTED:
                        if (homomorphic)
                            yEnc = yField.getEncrypted();
                        else
                            yPlain = (double) encryptionService.decryptLong(yField.getEncrypted(), version);
                        break;
                    default:
                        logger.fine("Skipping asset with no value for case " + yField.getFieldCase());
                        break;
                }

            // something weird happened
            if ((xPlain == null && xEnc == null) || (yPlain == null && yEnc == null))
                continue;
            count++;

            // both plaintext
            if (xPlain != null && yPlain != null) {
                sumX += xPlain;
                sumY += yPlain;
                sumXY += xPlain * yPlain;
                sumX2 += xPlain * xPlain;
                sumY2 += yPlain * yPlain;

            }
            // both encrypted
            else if (homomorphic && xEnc != null && yEnc != null) {
                
                if (canMultiply) {
                    encXList.add(xEnc);
                    encYList.add(yEnc);
                    encXYList.add(encryptionService.homomorphicMultiply(xEnc, yEnc, version));
                    encX2List.add(encryptionService.homomorphicMultiply(xEnc, xEnc, version));
                    encY2List.add(encryptionService.homomorphicMultiply(yEnc, yEnc, version));
                } else {
                    long x = encryptionService.decryptLong(xEnc, version);
                    long y = encryptionService.decryptLong(yEnc, version);
                    sumX += x;
                    sumY += y;
                    sumXY += (double) x * y;
                    sumX2 += (double) x * x;
                    sumY2 += (double) y * y;
                }
            }
            // one of the pair is encrypted – decrypt it
            else {
                if (xPlain == null)
                    xPlain = (double) encryptionService.decryptLong(xEnc, version);
                if (yPlain == null)
                    yPlain = (double) encryptionService.decryptLong(yEnc, version);

                sumX += xPlain;
                sumY += yPlain;
                sumXY += xPlain * yPlain;
                sumX2 += xPlain * xPlain;
                sumY2 += yPlain * yPlain;
            }
        } // end of the for loop processing assets, now we do maths

        // MATHS
        // homomorphic reduction of accumulated ciphertexts
        if (homomorphic) {
            //  X values
            if (!encXList.isEmpty()) {
                String sumEncX = encXList.size() == 1 ? encXList.get(0)
                        : encryptionService.homomorphicAdd(encXList, version);
                sumX += encryptionService.decryptLong(sumEncX, version);
            }

            // Y values
            if (!encYList.isEmpty()) {
                String sumEncY = encYList.size() == 1 ? encYList.get(0)
                        : encryptionService.homomorphicAdd(encYList, version);
                sumY += encryptionService.decryptLong(sumEncY, version);
            }

            // x*y values
            if (!encXYList.isEmpty()) {
                String sumEncXY = encXYList.size() == 1 ? encXYList.get(0)
                        : encryptionService.homomorphicAdd(encXYList, version);
                sumXY += encryptionService.decryptLong(sumEncXY, version);
            }

            // x*x values
            if (!encX2List.isEmpty()) {
                String sumEncX2 = encX2List.size() == 1 ? encX2List.get(0)
                        : encryptionService.homomorphicAdd(encX2List, version);
                sumX2 += encryptionService.decryptLong(sumEncX2, version);
            }

            // y*y values
            if (!encY2List.isEmpty()) {
                String sumEncY2 = encY2List.size() == 1 ? encY2List.get(0)
                        : encryptionService.homomorphicAdd(encY2List, version);
                sumY2 += encryptionService.decryptLong(sumEncY2, version);
            }
        }

        if (count < 2)
            return new RegressionResult(0, 0, 0, 0);

        // slope and intercept
        double slope = (count * sumXY - sumX * sumY) / (count * sumX2 - sumX * sumX);
        double intercept = (sumY - slope * sumX) / count;

        // r squared
        double meanY = sumY / count;
        double ssTot = sumY2 - 2 * meanY * sumY + count * meanY * meanY;
        // double ssRes = (count * sumXY - 2 * sumX * sumY + count * slope * slope * sumX2
        //         + count * intercept * intercept + 2 * slope * intercept * sumX
        //         - 2 * intercept * sumY);

        double ssRes = sumY2
             - 2 * slope * sumXY
             - 2 * intercept * sumY
             + slope * slope * sumX2
             + 2 * slope * intercept * sumX
             + intercept * intercept * count;

        double r2 = 1.0 - (ssRes / ssTot);

        return new RegressionResult(slope, intercept, r2, count);
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