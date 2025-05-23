package nl.medtechchain.chaincode.service.query.std;

import com.google.protobuf.Descriptors;
import nl.medtechchain.chaincode.service.encryption.PlatformEncryptionInterface;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import java.util.List;

public class IntegerStd implements Std{
    @Override
    public double std(PlatformEncryptionInterface encryptionInterface, Descriptors.FieldDescriptor descriptor,
    List<DeviceDataAsset> assets) {
        double sum = 0;
        double count = 0;

        // loop through assets in order to get the mean
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
            } 
        }
        // NOTE: we are not reusing the mean method, out of convinience
        // the count variable is handy as it count how much assets are actually there and not some outliers.
        double mean = sum / count;
        double variance = 0;

        // use that mean in the formula for standard deviation
        for(DeviceDataAsset a : assets){
            var fieldValue = (DeviceDataAsset.IntegerField) a.getDeviceData().getField(descriptor);
            switch(fieldValue.getFieldCase()){
                case PLAIN:
                    variance += Math.pow(fieldValue.getPlain() - mean, 2);
                    break;
                case ENCRYPTED:
                    // TODO: proper encryption handling!!!
                    if(encryptionInterface == null)
                        throw new IllegalStateException("Field " + descriptor.getName() + " is encrypted, but the platform is not properly configured to use encryption.");
                    variance += Math.pow(encryptionInterface.decryptLong(fieldValue.getEncrypted()) - mean, 2);
                    break;
                default:
                    // should not be reached
                    break;
            }
        }

        if(count < 2) return 0; // just in case to prevent possible division by 0
        return Math.sqrt(variance / (count - 1));
    }

    @Override
    public double mean(PlatformEncryptionInterface encryptionInterface, Descriptors.FieldDescriptor descriptor,
    List<DeviceDataAsset> assets) {
        double sum = 0;
        double count = 0;

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
                default:
                    // should not be reached
                    break;
            } 
        }

        return sum / count;
    }
    
}
