package nl.medtechchain.chaincode.service.query.std;

import java.time.Instant;
import java.util.List;


import com.google.protobuf.Descriptors;

import nl.medtechchain.chaincode.service.encryption.HomomorphicEncryptionScheme;
import nl.medtechchain.chaincode.service.encryption.PlatformEncryptionInterface;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;


public class TimestampStd implements Std{
    private long secsDay = 86400;
    @Override
    public MeanAndStd std(PlatformEncryptionInterface encryptionInterface, Descriptors.FieldDescriptor descriptor,
    List<DeviceDataAsset> asset) {
        double mean = this.mean(encryptionInterface, descriptor, asset);
        double variance = 0;
        long count = 0;
        for(DeviceDataAsset a : asset){
            DeviceDataAsset.TimestampField value = (DeviceDataAsset.TimestampField) a.getDeviceData().getField(descriptor);
            long secs = 0;
            switch(value.getFieldCase()){
                case PLAIN:
                    secs = value.getPlain().getSeconds();
                    count++;
                    break;
                case ENCRYPTED:
                    if(encryptionInterface == null)
                        throw new IllegalStateException("Field " + descriptor.getName() + " is encrypted, but the platform is not properly configured to use encryption.");
                    secs = encryptionInterface.decryptLong(value.getEncrypted());
                    count++;
                    break;
                default:
                    // skips this unset asset
                    continue;
            }

            variance += (secs - mean) * (secs - mean); 
        }

        if(count < 2) return new MeanAndStd(0, mean); 
        return new MeanAndStd(Math.sqrt(variance / (count - 1)), mean);
    }
    
    private double mean(PlatformEncryptionInterface encryptionInterface, Descriptors.FieldDescriptor descriptor,
    List<DeviceDataAsset> asset) {
        double sum = 0;
        long count = 0;
        String cryptoSum = null; 

        for(DeviceDataAsset a : asset){
            DeviceDataAsset.TimestampField value = (DeviceDataAsset.TimestampField) a.getDeviceData().getField(descriptor);
            switch(value.getFieldCase()){
                case PLAIN:
                    sum += value.getPlain().getSeconds();
                    count++;
                    break;
                case ENCRYPTED:
                    if(encryptionInterface == null)
                        throw new IllegalStateException("Field " + descriptor.getName() + " is encrypted, but the platform is not properly configured to use encryption.");
                    if (encryptionInterface.isHomomorphic()) {
                        if (cryptoSum == null)
                            cryptoSum = value.getEncrypted();
                        else
                            cryptoSum = ((HomomorphicEncryptionScheme) encryptionInterface).add(cryptoSum, value.getEncrypted());
                    }
                    else
                        sum += encryptionInterface.decryptLong(value.getEncrypted());
                    count++;
                    break;
                default:
                    break;
            }
        }

        if(cryptoSum != null) sum += encryptionInterface.decryptLong(cryptoSum);
        if(count == 0) return 0;
        double mean = sum / count;
        
        return mean;
    }
}

// "production_date, last_service_date, warranty_expiry_date, last_sync_time"
