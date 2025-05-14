package nl.medtechchain.chaincode.service.query.sum;

import com.google.protobuf.Descriptors;
import nl.medtechchain.chaincode.service.encryption.PlatformEncryptionInterface;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.devicedata.DeviceDataFieldType;
import java.util.List;

public interface Sum {
    long sum(PlatformEncryptionInterface enc, Descriptors.FieldDescriptor fd,
             List<DeviceDataAsset> assets);

    class Factory {
        public static Sum getInstance(DeviceDataFieldType t) {
            // we only work with usage_hours which is integer
            return new IntegerSum(); 
        }
    }
}