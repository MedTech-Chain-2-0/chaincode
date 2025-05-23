package nl.medtechchain.chaincode.service.query.std;

import java.time.Instant;
import java.util.List;


import com.google.protobuf.Descriptors;

import nl.medtechchain.chaincode.service.encryption.HomomorphicEncryptionScheme;
import nl.medtechchain.chaincode.service.encryption.PlatformEncryptionInterface;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;


public class TimestampStd implements Std{
    @Override
    public MeanAndStd std(PlatformEncryptionInterface encryptionInterface, Descriptors.FieldDescriptor descriptor,
    List<DeviceDataAsset> asset) {
        double mean = this.mean(encryptionInterface, descriptor, asset);
        double variance = 0;
        long count = 0;
        long now = Instant.now().getEpochSecond();
        for(DeviceDataAsset a : asset){
            DeviceDataAsset.TimestampField value = (DeviceDataAsset.TimestampField) a.getDeviceData().getField(descriptor);
            long secs = 0;
            switch(value.getFieldCase()){
                case PLAIN:
                    secs = value.getPlain().getSeconds();
                    count++;
                    break;
                case ENCRYPTED:
                    // TODO: proper encryption handling!!! yavor please fix this shit
                    if(encryptionInterface == null)
                        throw new IllegalStateException("Field " + descriptor.getName() + " is encrypted, but the platform is not properly configured to use encryption.");
                    secs = encryptionInterface.decryptLong(value.getEncrypted());
                    count++;
                    break;
                default:
                    // skips this unset asset, not contributing towards variance
                    continue;
            }

            double delta; // gonna be days, like why not 
            if(descriptor.getName().equals("warranty_expiry_date")){
                delta = secs - now; // because we looking back to the future
                delta /= 86400;
            }
            else{
                delta = now - secs; // we looking straight ahead towards the past
                delta /= 86400;
            }

            variance += (delta - mean) * (delta - mean); // heard a rummor that this is faster than math.pow
        }

        if(count < 2) return new MeanAndStd(0, mean); // maxguardprotect3000
        return new MeanAndStd(Math.sqrt(variance / (count - 1)), mean);
    }
    
    private double mean(PlatformEncryptionInterface encryptionInterface, Descriptors.FieldDescriptor descriptor,
    List<DeviceDataAsset> asset) {
        double sum = 0;
        long count = 0;
        String cryptoSum = null; // To be investigated if done right/ or fixed later on

        for(DeviceDataAsset a : asset){
            DeviceDataAsset.TimestampField value = (DeviceDataAsset.TimestampField) a.getDeviceData().getField(descriptor);
            switch(value.getFieldCase()){
                case PLAIN:
                    sum += value.getPlain().getSeconds();
                    count++;
                    break;
                case ENCRYPTED:
                    // TODO: proper encryption handling!!!
                    // this is done like the previous developers handled encryption case in average query
                    // average query might be dropped in the future, to be discussed
                    // seems logical but needs some special attention
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
        long now = Instant.now().getEpochSecond();
        double result = 0;

        if(descriptor.getName().equals("production_date"))
            result =  now - mean; // before how many seconds were the devices produced
        else if(descriptor.getName().equals("last_service_date"))
            result =  now -mean;
        else if(descriptor.getName().equals("warranty_expiry_date"))
            result = mean - now;
        else if(descriptor.getName().equals("last_sync_time"))
            result = now - mean;
        else
            // very much just in case
            throw new IllegalStateException("Unknown field: " + descriptor.getName());

        return result / 86400; // convert to days
    }
}

// "production_date, last_service_date, warranty_expiry_date, last_sync_time"
