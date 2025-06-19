package nl.medtechchain.chaincode.service.query;

import com.google.privacy.differentialprivacy.LaplaceNoise;
import com.google.protobuf.Descriptors;
import nl.medtechchain.chaincode.config.ConfigOps;
import nl.medtechchain.chaincode.service.differentialprivacy.MechanismType;
// import nl.medtechchain.chaincode.service.query.average.AverageQuery;
import nl.medtechchain.chaincode.service.query.count.CountQuery;
import nl.medtechchain.chaincode.service.query.groupedcount.GroupedCountQuery;
import nl.medtechchain.chaincode.service.query.linearregression.LinearRegressionQuery;
import nl.medtechchain.chaincode.service.query.sum.SumQuery;
import nl.medtechchain.chaincode.service.query.standarddeviation.STDQuery;
import nl.medtechchain.chaincode.service.query.uniquecount.UniqueCountQuery;
import nl.medtechchain.chaincode.service.query.histogram.HistogramQuery;
import nl.medtechchain.proto.common.ChaincodeError;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceCategory;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.devicedata.DeviceDataFieldType;
import nl.medtechchain.proto.devicedata.MedicalSpeciality;
import nl.medtechchain.proto.query.Filter;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;
import nl.medtechchain.proto.query.QueryResult.MeanAndStd;


import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

import static nl.medtechchain.chaincode.config.ConfigOps.PlatformConfigOps.get;
import static nl.medtechchain.chaincode.config.ConfigOps.PlatformConfigOps.getUnsafe;
import static nl.medtechchain.proto.config.PlatformConfig.Config.*;
import static nl.medtechchain.proto.query.Query.QueryType.*;

// Main query service - validates and executes queries with optional differential privacy
public class QueryService {
    
    private static final Logger logger = Logger.getLogger(QueryService.class.getName());
    
    private final PlatformConfig platformConfig;
    private final MechanismType mechanismType;
    
    public QueryService(PlatformConfig platformConfig) {
        this.platformConfig = platformConfig;
        
        String differentialPrivacyProp = get(platformConfig, CONFIG_FEATURE_QUERY_DIFFERENTIAL_PRIVACY).orElse("NONE");
        MechanismType type;
        try {
            type = MechanismType.valueOf(differentialPrivacyProp.toUpperCase());
        } catch (IllegalArgumentException e) {
            logger.warning("Invalid differential privacy mechanism: " + differentialPrivacyProp + ", defaulting to none");
            type = MechanismType.NONE;
        }
        this.mechanismType = type;
    }
        
    public Optional<ChaincodeError> validateQuery(Query query) {
        String validFields = "";
        switch (query.getQueryType()) {
            case COUNT:
                validFields = ConfigOps.PlatformConfigOps.get(platformConfig, CONFIG_FEATURE_QUERY_INTERFACE_COUNT_FIELDS).orElse("");
                break;
            case GROUPED_COUNT:
                validFields = ConfigOps.PlatformConfigOps.get(platformConfig, CONFIG_FEATURE_QUERY_INTERFACE_GROUPED_COUNT_FIELDS).orElse("");
                break;
            case AVERAGE:
                validFields = ConfigOps.PlatformConfigOps.get(platformConfig, CONFIG_FEATURE_QUERY_INTERFACE_AVERAGE_FIELDS).orElse("");
                break;
            case SUM:
                validFields = ConfigOps.PlatformConfigOps.get(platformConfig, CONFIG_FEATURE_QUERY_INTERFACE_SUM_FIELDS).orElse("");
                break;
            case UNIQUE_COUNT:
                validFields = ConfigOps.PlatformConfigOps.get(platformConfig, CONFIG_FEATURE_QUERY_INTERFACE_UNIQUE_COUNT_FIELDS).orElse("");
                break;
            case HISTOGRAM:
                validFields = ConfigOps.PlatformConfigOps.get(platformConfig, CONFIG_FEATURE_QUERY_INTERFACE_HISTOGRAM_FIELDS).orElse("");
                if(query.getBinSize() <= 0) return Optional.of(invalidQueryError("Bin size has to be bigger than 0"));
                break;
            case STD:
                validFields = ConfigOps.PlatformConfigOps.get(platformConfig, CONFIG_FEATURE_QUERY_INTERFACE_STD_FIELDS).orElse("");
                break;
            case LINEAR_REGRESSION:
                validFields = ConfigOps.PlatformConfigOps.get(platformConfig, CONFIG_FEATURE_QUERY_INTERFACE_LINEAR_REGRESSION_FIELDS_X).orElse("") + "," + ConfigOps.PlatformConfigOps.get(platformConfig, CONFIG_FEATURE_QUERY_INTERFACE_LINEAR_REGRESSION_FIELDS_Y).orElse("");
                break;
        }

        if (!validFields.contains(query.getTargetField()))
            return Optional.of(invalidQueryError("Target field not among valid fields for query type " + query.getTargetField() + " not in " + validFields + " for " + query.getQueryType().name()));

        if (deviceDataDescriptorByName(query.getTargetField()).isEmpty() && !query.getTargetField().equals("udi"))
            return Optional.of(invalidQueryError("Unknown target field: " + query.getTargetField()));

        for (Filter filter : query.getFiltersList()) {
            var fieldType = DeviceDataFieldTypeMapper.fromFieldName(filter.getField());

            if (filter.getField().equals(query.getTargetField()))
                return Optional.of(invalidQueryError("Target field specified as filter: " + query.getTargetField()));

            if (deviceDataDescriptorByName(filter.getField()).isEmpty())
                return Optional.of(invalidQueryError("Unknown filter field: " + filter.getField()));

            var invalidFilter = Optional.of(invalidQueryError("Invalid filter type: " + filter.getField() + " " + fieldType + "!=" + filter.getComparatorCase()));

            switch (filter.getComparatorCase()) {
                case ENUM_FILTER:
                    if (fieldType == DeviceDataFieldType.DEVICE_CATEGORY)
                        try {
                            DeviceCategory.valueOf(filter.getEnumFilter().getValue());
                        } catch (Throwable t) {
                            return invalidFilter.map(e -> e.toBuilder().setDetails(e.getDetails() + " " + t.getMessage()).build());
                        }

                    if (fieldType == DeviceDataFieldType.MEDICAL_SPECIALITY)
                        try {
                            MedicalSpeciality.valueOf(filter.getEnumFilter().getValue());
                        } catch (Throwable t) {
                            return invalidFilter.map(e -> e.toBuilder().setDetails(e.getDetails() + " " + t.getMessage()).build());
                        }
                    break;
                case STRING_FILTER:
                    if (fieldType != DeviceDataFieldType.STRING)
                        return invalidFilter;
                    break;
                case INTEGER_FILTER:
                    if (fieldType != DeviceDataFieldType.INTEGER)
                        return invalidFilter;
                    break;
                case BOOL_FILTER:
                    if (fieldType != DeviceDataFieldType.BOOL)
                        return invalidFilter;
                    break;
                case TIMESTAMP_FILTER:
                    if (fieldType != DeviceDataFieldType.TIMESTAMP)
                        return invalidFilter;
                    break;
                case COMPARATOR_NOT_SET:
                    return Optional.of(invalidQueryError("Comparator for filter not set: " + filter.getField()));
            }
        }

        return Optional.empty();
    }
    
    
    public QueryResult sum(Query query, List<DeviceDataAsset> assets) {
        var fieldType = DeviceDataFieldTypeMapper.fromFieldName(query.getTargetField());

        if (fieldType != DeviceDataFieldType.INTEGER)
            throw new IllegalStateException("cannot run SUM over " + fieldType);

        QueryResult result = new SumQuery(platformConfig).process(query, assets);

        if (mechanismType == MechanismType.LAPLACE) {
            var noise = new LaplaceNoise();
            long noisySum = noise.addNoise(result.getSumResult(), 1, getEpsilon(), 0);
            result = QueryResult.newBuilder().setSumResult(noisySum).build();
        }

        return result;
    }
    
    // TODO: Implement these using the new architecture
    
    public QueryResult count(Query query, List<DeviceDataAsset> assets) {
        QueryResult result = new CountQuery(platformConfig).process(query, assets);
        
        if (mechanismType == MechanismType.LAPLACE) {
            var noise = new LaplaceNoise();
            int noisyCount = Math.abs((int) noise.addNoise(result.getCountResult(), 1, getEpsilon(), 0));
            result = QueryResult.newBuilder().setCountResult(noisyCount).build();
        }
        
        return result;
    }
    
    public QueryResult groupedCount(Query query, List<DeviceDataAsset> assets) {
        QueryResult result = new GroupedCountQuery(platformConfig).process(query, assets);
        
        if (mechanismType == MechanismType.LAPLACE) {
            var noise = new LaplaceNoise();
            // Apply noise to each group count
            QueryResult.GroupedCount.Builder noisyBuilder = QueryResult.GroupedCount.newBuilder();
            result.getGroupedCountResult().getMapMap().forEach((key, value) -> 
                noisyBuilder.putMap(key, Math.abs(noise.addNoise(value, 1, getEpsilon(), null)))
            );
            result = QueryResult.newBuilder().setGroupedCountResult(noisyBuilder.build()).build();
        }
        
        return result;
    }
    
    // public QueryResult average(Query query, List<DeviceDataAsset> assets) {
    //     var fieldType = DeviceDataFieldTypeMapper.fromFieldName(query.getTargetField());

    //     if (fieldType != DeviceDataFieldType.INTEGER && fieldType != DeviceDataFieldType.TIMESTAMP) {
    //         throw new IllegalStateException(
    //             "Cannot run AVERAGE over " + fieldType + ". " +
    //             "Only numeric and timestamp fields are supported for average calculations."
    //         );
    //     }

    //     QueryResult result = new AverageQuery(platformConfig).process(query, assets);

    //     if (mechanismType == MechanismType.LAPLACE) {
    //         var noise = new LaplaceNoise();
    //         double noisyAverage = noise.addNoise(result.getAverageResult(), 1, getEpsilon(), 0);
    //         result = QueryResult.newBuilder().setAverageResult(noisyAverage).build();
    //     }

    //     return result; 
    // }
    
    public QueryResult uniqueCount(Query query, List<DeviceDataAsset> assets) {
        var fieldType = DeviceDataFieldTypeMapper.fromFieldName(query.getTargetField());
        
        QueryResult result = new UniqueCountQuery(platformConfig).process(query, assets);
        
        if (mechanismType == MechanismType.LAPLACE) {
            var noise = new LaplaceNoise();
            int noisyCount = Math.abs((int) noise.addNoise(result.getCountResult(), 1, getEpsilon(), 0));
            result = QueryResult.newBuilder().setCountResult(noisyCount).build();
        }
        
        return result;
    }
    
    public QueryResult histogram(Query query, List<DeviceDataAsset> assets) {
        var fieldType = DeviceDataFieldTypeMapper.fromFieldName(query.getTargetField());

        // Only allow histogram for INTEGER and TIMESTAMP fields
        if (fieldType != DeviceDataFieldType.INTEGER && fieldType != DeviceDataFieldType.TIMESTAMP) {
            throw new IllegalStateException("Cannot run HISTOGRAM over " + fieldType + 
                ". Only INTEGER and TIMESTAMP fields are supported.");
        }

        QueryResult result = new HistogramQuery(platformConfig, query.getBinSize()).process(query, assets);

        if (mechanismType == MechanismType.LAPLACE) {
            var noise = new LaplaceNoise();
            // Apply noise to each bin count
            var noisyBuilder = QueryResult.GroupedCount.newBuilder();
            result.getGroupedCountResult().getMapMap().forEach((key, value) -> 
                noisyBuilder.putMap(key, Math.abs(noise.addNoise(value, 1, getEpsilon(), null)))
            );
            result = QueryResult.newBuilder().setGroupedCountResult(noisyBuilder.build()).build();
        }

        return result;
    }
    
    public QueryResult std(Query query, List<DeviceDataAsset> assets) {
        var fieldType = DeviceDataFieldTypeMapper.fromFieldName(query.getTargetField());

        if (fieldType != DeviceDataFieldType.INTEGER && fieldType != DeviceDataFieldType.TIMESTAMP)
            throw new IllegalStateException("cannot run STD over " + fieldType);

        QueryResult result = new STDQuery(platformConfig).process(query, assets);

        if (mechanismType == MechanismType.LAPLACE) {
            var noise = new LaplaceNoise();
            double noisyMean = noise.addNoise(result.getMeanStd().getMean(), 1, getEpsilon(), 0);
            double noisyStd = noise.addNoise(result.getMeanStd().getStd(), 1, getEpsilon(), 0);
            MeanAndStd meanAndStd = MeanAndStd.newBuilder().setMean(noisyMean).setStd(noisyStd).build();
            result = QueryResult.newBuilder().setMeanStd(meanAndStd).build();
        }

        return result;
    }
    
    public QueryResult linearRegression(Query query, List<DeviceDataAsset> assets) {
        var fieldType = DeviceDataFieldTypeMapper.fromFieldName(query.getTargetField());

        if (fieldType != DeviceDataFieldType.INTEGER && fieldType != DeviceDataFieldType.TIMESTAMP)
            throw new IllegalStateException("cannot run LINEAR_REGRESSION over " + fieldType);

        QueryResult result = new LinearRegressionQuery(platformConfig).process(query, assets);

        return result;
    }
    
    // helpers
    
    private double getEpsilon() {
        return Double.parseDouble(getUnsafe(platformConfig, CONFIG_FEATURE_QUERY_DIFFERENTIAL_PRIVACY_LAPLACE_EPSILON));
    }

    private ChaincodeError invalidQueryError(String details) {
        return ChaincodeError.newBuilder().setCode(ChaincodeError.ErrorCode.INVALID_TRANSACTION).setMessage("Bad query").setDetails(details).build();
    }

    private Optional<Descriptors.FieldDescriptor> deviceDataDescriptorByName(String name) {
        return Optional.ofNullable(DeviceDataAsset.DeviceData.getDescriptor().findFieldByName(name));
    }
} 