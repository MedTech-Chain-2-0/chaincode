package nl.medtechchain.chaincode.service.query.std;

import com.google.protobuf.Descriptors;
import nl.medtechchain.chaincode.service.encryption.PlatformEncryptionInterface;
import nl.medtechchain.chaincode.service.encryption.PlatformBfvEncryption;
import nl.medtechchain.chaincode.service.encryption.HomomorphicEncryptionScheme;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import java.util.List;
import java.util.ArrayList;

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

        List<String> encrypted = new ArrayList<>();

        // loop through assets
        for(DeviceDataAsset a : assets){
            var value = (DeviceDataAsset.IntegerField) a.getDeviceData().getField(descriptor);
            switch(value.getFieldCase()){
                case PLAIN:
                    sum += value.getPlain();
                    count++;
                    break;
                case ENCRYPTED:
                    if(encryptionInterface == null)
                        throw new IllegalStateException("Field " + descriptor.getName() + " is encrypted, but the platform is not properly configured to use encryption.");
                    if (encryptionInterface.isHomomorphic()) 
                        encrypted.add(value.getEncrypted());
                    else {
                        // no homomorphic encryption -> we have to decrypt
                        sum += encryptionInterface.decryptLong(value.getEncrypted());
                    }
                    count++;
                    break;
                default:
                    // should not be reached
                    break;
            } 
        }

        
        if (!encrypted.isEmpty()) {
            String ctSum;
            if (encrypted.size() == 1) {
                ctSum = encrypted.get(0);
            }
            else if (encryptionInterface instanceof PlatformBfvEncryption)
                ctSum = ((PlatformBfvEncryption)encryptionInterface).addAll(encrypted);
            else {                               
                ctSum = encrypted.get(0);
                for (int i = 1; i < encrypted.size(); i++)
                    ctSum = ((HomomorphicEncryptionScheme)encryptionInterface).add(ctSum, encrypted.get(i));
            }
            sum += encryptionInterface.decryptLong(ctSum);
        }

        if (count == 0) return 0;

        return sum / count;
    }
    
}
