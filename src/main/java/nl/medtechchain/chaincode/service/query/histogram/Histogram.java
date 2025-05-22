package nl.medtechchain.chaincode.service.query.histogram;

import com.google.protobuf.Descriptors;

import nl.medtechchain.chaincode.service.encryption.PlatformEncryptionInterface;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.devicedata.DeviceDataFieldType;
import java.util.List;
import java.util.Map;

public interface Histogram {
    Map<String, Long> histogram(PlatformEncryptionInterface encryptionInterface, Descriptors.FieldDescriptor descriptor, List<DeviceDataAsset> assets, long binSize);

    // the factory pattern the basic queries use,
    // only two possible field types to take care of
    class Factory{
        public static Histogram getInstance(DeviceDataFieldType type){
            switch(type){
                case INTEGER:
                    return new IntegerHistogram();
                case TIMESTAMP:
                    return new TimestampHistogram();
                default:
                    // should not be reached
                    return (encryptionInterface, descriptor, assets, binSize) -> Map.of();
            }
        }
    }
}

