package nl.medtechchain.chaincode.service.query.std;

import java.util.List;

import com.google.protobuf.Descriptors;

import nl.medtechchain.chaincode.service.encryption.HomomorphicEncryptionScheme;
import nl.medtechchain.chaincode.service.encryption.PlatformEncryptionInterface;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.devicedata.DeviceDataFieldType;

public class TimestampStd implements Std{
    @Override
    public double std(PlatformEncryptionInterface encryptionInterface, Descriptors.FieldDescriptor descriptor,
    List<DeviceDataAsset> asset) {
        return 0;
    }
    
    @Override
    public double mean(PlatformEncryptionInterface encryptionInterface, Descriptors.FieldDescriptor descriptor,
    List<DeviceDataAsset> asset) {
        double sum = 0;
        double count = 0;
        String cryptoSum = null; // To be investigated if done right/ or fixed later on

        for(DeviceDataAsset a : asset){
            var value = (DeviceDataAsset.TimestampField) a.getDeviceData().getField(descriptor);
            long secs = 0;
            switch(value.getFieldCase()){
                case PLAIN:
                    secs =value.getPlain().getSeconds();
                    count++;
                case ENCRYPTED:
                    // TODO: proper encryption handling!!!
                    // this is done like the previous developers handled encryption case in average query
                    // average query might be dropped in the future, to be discussed
                    // seems logical but needs some special attention
                    if(encryptionInterface == null)
                        throw new IllegalStateException("Field " + descriptor.getName() + " is encrypted, but the platform is not properly configured to use encryption.");
                    cryptoSum = ((HomomorphicEncryptionScheme) encryptionInterface).add(cryptoSum, value.getEncrypted());
                    count++;
                    if (encryptionInterface.isHomomorphic()) {
                        if (cryptoSum == null)
                            cryptoSum = value.getEncrypted();
                        else
                            cryptoSum = ((HomomorphicEncryptionScheme) encryptionInterface).add(cryptoSum, value.getEncrypted());
                    }
                    else
                        sum += encryptionInterface.decryptLong(value.getEncrypted());
            }
        }

        if(cryptoSum != null) sum += encryptionInterface.decryptLong(cryptoSum);

        return sum / count;
    }
}



// "production_date, last_service_date, warranty_expiry_date, last_sync_time"