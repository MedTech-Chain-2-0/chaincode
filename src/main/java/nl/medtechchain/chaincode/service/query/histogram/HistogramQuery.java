package nl.medtechchain.chaincode.service.query.histogram;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.Descriptors;

import nl.medtechchain.chaincode.service.query.QueryProcessor;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;

public class HistogramQuery extends QueryProcessor {

    private final long binSize;

    public HistogramQuery(PlatformConfig platformConfig, long binSize) {
        super(platformConfig);
        this.binSize = binSize;
    }

    @Override
    public QueryResult process(Query query, List<DeviceDataAsset> assets) {
        var fieldDescriptor = getFieldDescriptor(query.getTargetField());
        if (fieldDescriptor == null) {
            throw new IllegalArgumentException("Unknown target field: " + query.getTargetField());
        }

        // Group by version for optimal processing
        Map<String, List<DeviceDataAsset>> versionGroups = groupByVersion(assets);
        Map<String, Long> histogramBins = new HashMap<>();

        // First pass: find min and max values if needed
        Range range = determineRange(assets, fieldDescriptor);
        if (range == null) {
            return QueryResult.newBuilder()
                    .setGroupedCountResult(QueryResult.GroupedCount.newBuilder().build())
                    .build();
        }

        // Process each version group
        for (Map.Entry<String, List<DeviceDataAsset>> entry : versionGroups.entrySet()) {
            String version = entry.getKey();
            List<DeviceDataAsset> versionAssets = entry.getValue();

            logger.fine("Processing " + versionAssets.size() + " assets with version: " + version);
            processVersionGroup(versionAssets, fieldDescriptor, version, histogramBins, range);
        }

        // Convert histogram to GroupedCountResult
        return QueryResult.newBuilder().setGroupedCountResult(QueryResult.GroupedCount.newBuilder().putAllMap(histogramBins).build()).build();
    }

    private void processVersionGroup(List<DeviceDataAsset> assets,
            Descriptors.FieldDescriptor fieldDescriptor,
            String version,
            Map<String, Long> histogramBins,
            Range range) {
        // For homomorphic encryption, we'll collect all encrypted values
        List<String> encryptedValues = new ArrayList<>();

        long effectiveBinSize = this.binSize;

        // Peek at the first asset to decide the field type
        if (!assets.isEmpty()) {
            Object sampleFieldValue = assets.get(0).getDeviceData().getField(fieldDescriptor);
            if (sampleFieldValue instanceof DeviceDataAsset.TimestampField) {
                effectiveBinSize *= 86400; // seconds in a day
            }
        }

        for (DeviceDataAsset asset : assets) {
            Object fieldValue = asset.getDeviceData().getField(fieldDescriptor);

            if (fieldValue instanceof DeviceDataAsset.IntegerField) {
                processIntegerField((DeviceDataAsset.IntegerField) fieldValue, version, histogramBins, encryptedValues, range, effectiveBinSize);
            } else if (fieldValue instanceof DeviceDataAsset.TimestampField) {
                processTimestampField((DeviceDataAsset.TimestampField) fieldValue, version, histogramBins, encryptedValues, range, effectiveBinSize);
            }
        }

        // Process homomorphic values if we have any
        if (!encryptedValues.isEmpty() && encryptionService != null && encryptionService.isHomomorphic()) {
            // Now decrypt each value individually to determine its bin
            for (String encrypted : encryptedValues) {
                long value = encryptionService.decryptLong(encrypted, version);
                String bin = getBinLabel(value, range, effectiveBinSize);
                histogramBins.merge(bin, 1L, Long::sum);
            }
        }
    }

    private void processIntegerField(DeviceDataAsset.IntegerField field,
            String version,
            Map<String, Long> histogramBins,
            List<String> encryptedValues,
            Range range,
            long effectiveBinSize) {
        switch (field.getFieldCase()) {
            case PLAIN:
                long value = field.getPlain();
                String bin = getBinLabel(value, range,effectiveBinSize);
                histogramBins.merge(bin, 1L, Long::sum);
                break;

            case ENCRYPTED:
                if (encryptionService == null) {
                    throw new IllegalStateException("Found encrypted data but no encryption service configured. "
                            + "Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME to 'paillier' or 'bfv'.");
                }

                if (encryptionService.isHomomorphic()) {
                    // For homomorphic encryption, we'll decrypt values in batch later
                    encryptedValues.add(field.getEncrypted());
                } else {
                    // For non-homomorphic encryption, decrypt and bin immediately
                    long decryptedValue = encryptionService.decryptLong(field.getEncrypted(), version);
                    String decryptedBin = getBinLabel(decryptedValue, range,effectiveBinSize);
                    histogramBins.merge(decryptedBin, 1L, Long::sum);
                }
                break;
        }
    }

    private void processTimestampField(DeviceDataAsset.TimestampField field,
            String version,
            Map<String, Long> histogramBins,
            List<String> encryptedValues,
            Range range,
            long effectiveBinSize) {
        switch (field.getFieldCase()) {
            case PLAIN:
                long seconds = field.getPlain().getSeconds();
                String bin = getBinLabel(seconds, range,effectiveBinSize);
                histogramBins.merge(bin, 1L, Long::sum);
                break;

            case ENCRYPTED:
                if (encryptionService == null) {
                    throw new IllegalStateException("Found encrypted data but no encryption service configured. "
                            + "Set CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME to 'paillier' or 'bfv'.");
                }

                if (encryptionService.isHomomorphic()) {
                    // For homomorphic encryption, we'll decrypt values in batch later
                    encryptedValues.add(field.getEncrypted());
                } else {
                    // For non-homomorphic encryption, decrypt and bin immediately
                    long decryptedSeconds = encryptionService.decryptLong(field.getEncrypted(), version);
                    String decryptedBin = getBinLabel(decryptedSeconds, range,effectiveBinSize);
                    histogramBins.merge(decryptedBin, 1L, Long::sum);
                }
                break;
        }
    }

    private Range determineRange(List<DeviceDataAsset> assets, Descriptors.FieldDescriptor fieldDescriptor) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        boolean hasValues = false;

        for (DeviceDataAsset asset : assets) {
            Object fieldValue = asset.getDeviceData().getField(fieldDescriptor);
            Long value = null;

            if (fieldValue instanceof DeviceDataAsset.IntegerField) {
                DeviceDataAsset.IntegerField field = (DeviceDataAsset.IntegerField) fieldValue;
                if (field.getFieldCase() == DeviceDataAsset.IntegerField.FieldCase.PLAIN) {
                    value = field.getPlain();
                } else if (field.getFieldCase() == DeviceDataAsset.IntegerField.FieldCase.ENCRYPTED && encryptionService != null) {
                    value = encryptionService.decryptLong(field.getEncrypted(), asset.getKeyVersion());
                }
            } else if (fieldValue instanceof DeviceDataAsset.TimestampField) {
                DeviceDataAsset.TimestampField field = (DeviceDataAsset.TimestampField) fieldValue;
                if (field.getFieldCase() == DeviceDataAsset.TimestampField.FieldCase.PLAIN) {
                    value = field.getPlain().getSeconds();
                } else if (field.getFieldCase() == DeviceDataAsset.TimestampField.FieldCase.ENCRYPTED && encryptionService != null) {
                    value = encryptionService.decryptLong(field.getEncrypted(), asset.getKeyVersion());
                }
            }

            if (value != null) {
                min = Math.min(min, value);
                max = Math.max(max, value);
                hasValues = true;
            }
        }

        return hasValues ? new Range(min, max) : null;
    }

    private String getBinLabel(long value, Range range, long effectiveBinSize) {
        // Calculate bin start based on the value and bin size
        long binStart = (value - range.min) / effectiveBinSize * effectiveBinSize + range.min;
        long binEnd = binStart + effectiveBinSize - 1;

        return String.format("%d-%d", binStart, binEnd);
    }

    private static class Range {

        final long min;
        final long max;

        Range(long min, long max) {
            this.min = min;
            this.max = max;
        }
    }
}
