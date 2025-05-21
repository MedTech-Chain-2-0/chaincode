package nl.medtechchain.chaincode.service.query.histogram;

import java.util.List;
import java.util.Map;

import com.google.protobuf.Descriptors.FieldDescriptor;

import nl.medtechchain.chaincode.service.encryption.PlatformEncryptionInterface;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import com.google.protobuf.Descriptors;

import java.util.HashMap;


public class TimestampHistogram implements Histogram{

    @Override
    public Map<String, Long> histogram(PlatformEncryptionInterface encryptionInterface, FieldDescriptor descriptor,
            List<DeviceDataAsset> assets, long binSize) {
        long bin = binSize * 86400;
        // TODO: add support for nicely picked timestamp bins, currently they start from 16th of December
        // might also get handled front end side
        Map<String, Long> result = new HashMap<String, Long>();
        for(DeviceDataAsset asset : assets){
            DeviceDataAsset.TimestampField field = (DeviceDataAsset.TimestampField) asset.getDeviceData().getField(descriptor);
            long value = 0;
            switch(field.getFieldCase()){
                case PLAIN:
                    value = field.getPlain().getSeconds();
                    break;
                case ENCRYPTED:
                    value = encryptionInterface.decryptLong(field.getEncrypted());
                    break;
                default:
                    value = 0;
                    break;
            }

            long start = (value / bin) * bin;
            String key = start + " - " + (start + bin - 1);
            result.put(key, result.getOrDefault(key, 0L) + 1);
        }

        return result;
    }
    
}
