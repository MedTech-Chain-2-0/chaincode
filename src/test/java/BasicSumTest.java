package nl.medtechchain.chaincode.test;

import org.junit.jupiter.api.Test;

import com.google.auto.value.extension.toprettystring.ToPrettyString;
import com.google.privacy.differentialprivacy.LaplaceNoise;
import com.google.protobuf.Descriptors;
import nl.medtechchain.chaincode.config.ConfigOps;
import nl.medtechchain.chaincode.service.differentialprivacy.MechanismType;
import nl.medtechchain.chaincode.service.encryption.PlatformEncryptionInterface;
import nl.medtechchain.chaincode.service.query.average.Average;
import nl.medtechchain.chaincode.service.query.groupedcount.GroupedCount;
import nl.medtechchain.chaincode.service.query.histogram.Histogram;
import nl.medtechchain.chaincode.service.query.std.Std;
import nl.medtechchain.chaincode.service.query.sum.Sum;
import nl.medtechchain.chaincode.service.query.uniquecount.UniqueCount;
import nl.medtechchain.proto.common.ChaincodeError;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceCategory;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.devicedata.DeviceDataFieldType;
import nl.medtechchain.proto.devicedata.MedicalSpeciality;
import nl.medtechchain.proto.query.Filter;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;
import nl.medtechchain.chaincode.service.query.QueryService;
import nl.medtechchain.proto.query.QueryResult.MeanAndStd;
import java.util.*;
import org.junit.jupiter.api.Assertions;
import nl.medtechchain.chaincode.test.TestDataGenerator;

public class BasicSumTest {
    private Query buildSumQuery(String field) {
        return Query.newBuilder()
            .setQueryType(Query.QueryType.SUM)
            .setTargetField(field)
            .build();
    }

    private long sum(List<DeviceDataAsset> assets){
        PlatformConfig platformConfig = PlatformConfig.newBuilder().build();
        QueryService queryService = new QueryService(platformConfig);
        Query query = buildSumQuery("usage_hours");
        QueryResult result = queryService.sum(query, assets);
        return result.getSumResult();
    }

    @Test 
    public void t1(){
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> countMap = new HashMap<>();
        countMap.put(100, 1);
        long expected = 100;
        spec.put("usage_hours", countMap);
        TestDataGenerator generator = new TestDataGenerator();
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);
        long result = sum(assets);
        Assertions.assertEquals(expected, result, "Sum is different than expected");
    }
    

}
