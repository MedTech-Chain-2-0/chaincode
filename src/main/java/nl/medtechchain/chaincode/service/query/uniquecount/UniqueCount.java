package nl.medtechchain.chaincode.service.query.uniquecount;

import com.google.protobuf.Descriptors;
import nl.medtechchain.chaincode.service.encryption.PlatformEncryptionInterface;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.devicedata.DeviceDataFieldType;
import java.util.List;


public interface UniqueCount {
    long uniqueCount(PlatformEncryptionInterface enc,
                     Descriptors.FieldDescriptor fd,
                     List<DeviceDataAsset> assets);

    class Factory {
        public static UniqueCount getInstance(DeviceDataFieldType td) {
            switch (td) {
                case STRING:
                    return new StringUniqueCount();
                case DEVICE_CATEGORY:
                case MEDICAL_SPECIALITY:
                    return new EnumUniqueCount();

                default:
                    throw new IllegalArgumentException("unsupported field type: " + td);
            }
        }
    }
}