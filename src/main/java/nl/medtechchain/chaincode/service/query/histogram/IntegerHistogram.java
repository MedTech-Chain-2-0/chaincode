package nl.medtechchain.chaincode.service.query.histogram;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.Descriptors.FieldDescriptor;

import nl.medtechchain.chaincode.service.encryption.PlatformEncryptionInterface;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;

public class IntegerHistogram implements Histogram {

    @Override
    public Map<String, Long> histogram(PlatformEncryptionInterface encryptionInterface, FieldDescriptor descriptor,
            List<DeviceDataAsset> assets, long binSize) {

        Map<String, Long> result = new HashMap<String, Long>();
        for(DeviceDataAsset asset : assets) {
            DeviceDataAsset.IntegerField field = (DeviceDataAsset.IntegerField) asset.getDeviceData().getField(descriptor);
            long value = 0;
            switch(field.getFieldCase()){
                case PLAIN:
                    value = field.getPlain();
                    break;

                case ENCRYPTED:
                    if(encryptionInterface == null) {
                        throw new IllegalStateException("Field " + descriptor.getName() + " is encrypted, but the platform is not properly configured to use encryption.");
                    }
                    value = encryptionInterface.decryptLong(field.getEncrypted());
                    // TODO: add homomorphic operations
                    break;

                default:
                    // should not be reached.
                    value = 0;
                    break;
            }
            
            long start = (value / binSize) * binSize; // gets the start of the bin
            String bin = start + " - " + (start + binSize - 1);
            result.put(bin, result.getOrDefault(bin, 0L) + 1);
        }
        return result;
    }
    
}
