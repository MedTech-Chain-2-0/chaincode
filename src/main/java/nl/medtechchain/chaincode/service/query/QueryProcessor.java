package nl.medtechchain.chaincode.service.query;

import com.google.protobuf.Descriptors;
import nl.medtechchain.chaincode.service.encryption.EncryptionService;
import nl.medtechchain.chaincode.service.encryption.EncryptionServiceFactory;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.query.Query;
import nl.medtechchain.proto.query.QueryResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

// Base class for query processors 
public abstract class QueryProcessor {
    protected final EncryptionService encryptionService; // can be null if no encryption configured
    protected final Logger logger;
    
    public QueryProcessor(PlatformConfig platformConfig) {
        this.encryptionService = EncryptionServiceFactory.create(platformConfig);
        this.logger = Logger.getLogger(getClass().getName());
    }
    
    // Each query type implements this however they want
    public abstract QueryResult process(Query query, List<DeviceDataAsset> assets);
    
    protected Map<String, List<DeviceDataAsset>> groupByVersion(List<DeviceDataAsset> assets) {
        Map<String, List<DeviceDataAsset>> groups = new HashMap<>();
        for (DeviceDataAsset asset : assets) {
            String version = asset.getKeyVersion();
            groups.computeIfAbsent(version, k -> new ArrayList<>()).add(asset);
        }
        return groups;
    }
    
    // just a helper to get protobuf field descriptors
    protected Descriptors.FieldDescriptor getFieldDescriptor(String fieldName) {
        return DeviceDataAsset.DeviceData.getDescriptor().findFieldByName(fieldName);
    }
} 