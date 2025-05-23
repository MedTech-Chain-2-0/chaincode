package nl.medtechchain.chaincode.service.query.std;

import java.util.List;

import com.google.protobuf.Descriptors;
import nl.medtechchain.chaincode.service.encryption.PlatformEncryptionInterface;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.devicedata.DeviceDataFieldType;

public interface Std {
    double std(PlatformEncryptionInterface encryptionInterface, Descriptors.FieldDescriptor descriptor,
    List<DeviceDataAsset> asset);
    double mean(PlatformEncryptionInterface encryptionInterface, Descriptors.FieldDescriptor descriptor,
    List<DeviceDataAsset> asset);

    class Factory{
        public static Std getInstance(DeviceDataFieldType type){
            switch(type){
                case INTEGER:
                    return new IntegerStd();
                case TIMESTAMP:
                    return new TimestampStd();
                default:
                    // should not be reached
                    // maybe throw smth idk
                    return null;
            }
        }
    }
}
