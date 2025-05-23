package nl.medtechchain.chaincode.service.query.std;

import java.util.List;

import com.google.protobuf.Descriptors;
import nl.medtechchain.chaincode.service.encryption.PlatformEncryptionInterface;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.devicedata.DeviceDataFieldType;

public interface Std {
    class MeanAndStd {
        private double std;
        private double mean;
        public MeanAndStd(double std, double mean){
            this.std = std;
            this.mean = mean;
        }
        public double mean(){
            return mean;
        }
        public double std(){
            return std;
        }
    }

    MeanAndStd std(PlatformEncryptionInterface encryptionInterface, Descriptors.FieldDescriptor descriptor,
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
                    return (encryptionInterface, descriptor, asset) -> new MeanAndStd(0, 0);
            }
        }
    }
}
