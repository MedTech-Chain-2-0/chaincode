package nl.medtechchain.chaincode.service.query.std;

import com.google.protobuf.Descriptors;
import nl.medtechchain.chaincode.service.encryption.PlatformEncryptionInterface;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import java.util.List;

public class IntegerStd implements Std{
    @Override
    public MeanAndStd std(PlatformEncryptionInterface encryptionInterface, Descriptors.FieldDescriptor descriptor,
    List<DeviceDataAsset> assets) {
        double mean = this.mean(encryptionInterface, descriptor, assets);
        double variance = 0;
        long count = 0;

        // use that mean in the formula for standard deviation
        for(DeviceDataAsset a : assets){
            var fieldValue = (DeviceDataAsset.IntegerField) a.getDeviceData().getField(descriptor);
            switch(fieldValue.getFieldCase()){
                case PLAIN:
                    variance += Math.pow(fieldValue.getPlain() - mean, 2);
                    count++;
                    break;
                case ENCRYPTED:
                    // TODO: proper encryption handling!!!
                    if(encryptionInterface == null)
                        throw new IllegalStateException("Field " + descriptor.getName() + " is encrypted, but the platform is not properly configured to use encryption.");
                    variance += Math.pow(encryptionInterface.decryptLong(fieldValue.getEncrypted()) - mean, 2);
                    count++;
                    break;
                default:
                    // should not be reached
                    break;
            }
        }

        if(count < 2) return new MeanAndStd(0, mean); // just in case to prevent possible division by 0
        double std = Math.sqrt(variance / (count - 1));
        return new MeanAndStd(std, mean);
    }

    private double mean(PlatformEncryptionInterface encryptionInterface, Descriptors.FieldDescriptor descriptor,
    List<DeviceDataAsset> assets) {
        long count = 0;
        double sum = 0;

        // loop through assets
        for(DeviceDataAsset a : assets){
            var value = (DeviceDataAsset.IntegerField) a.getDeviceData().getField(descriptor);
            switch(value.getFieldCase()){
                case PLAIN:
                    sum += value.getPlain();
                    count++;
                    break;
                case ENCRYPTED:
                    // TODO: proper encryption handling!!!
                    if(encryptionInterface == null)
                        throw new IllegalStateException("Field " + descriptor.getName() + " is encrypted, but the platform is not properly configured to use encryption.");
                    sum += encryptionInterface.decryptLong(value.getEncrypted());
                    count++;
                    break;
                default:
                    // should not be reached
                    break;
            } 
        }

        if (count == 0) return 0;

        return sum / count;
    }
    
}
