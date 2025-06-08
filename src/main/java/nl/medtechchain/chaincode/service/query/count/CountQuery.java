package nl.medtechchain.chaincode.service.query.count;

import nl.medtechchain.chaincode.service.query.QueryProcessor;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;

import java.util.List;

// Counts total number of assets
public class CountQuery extends QueryProcessor {
    
    public CountQuery(PlatformConfig platformConfig) {
        super(platformConfig);
    }
    
    @Override
    public QueryResult process(Query query, List<DeviceDataAsset> assets) {
        int count = assets.size();
        logger.info("Total asset count: " + count);
        
        return QueryResult.newBuilder().setCountResult(count).build();
    }
} 