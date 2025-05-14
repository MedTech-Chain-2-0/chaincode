package nl.medtechchain.chaincode.service.query.uniquecount;

import com.google.protobuf.Descriptors;
import nl.medtechchain.chaincode.service.encryption.HomomorphicEncryptionScheme;
import nl.medtechchain.chaincode.service.encryption.PlatformEncryptionInterface;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

public class StringUniqueCount implements UniqueCount {
    @Override
    public long uniqueCount(PlatformEncryptionInterface encryptionInterface,
                            Descriptors.FieldDescriptor descriptor,
                            List<DeviceDataAsset> assets) {

        Set<String> uniq = new HashSet<>();

        // same logic as all other queries
        // loop through the devices, add the field to set, return the size of set.
        for (DeviceDataAsset asset: assets) {
            var val = (DeviceDataAsset.StringField)
                      asset.getDeviceData().getField(descriptor);

            String s;
            switch (val.getFieldCase()) {
                case PLAIN:
                    s = val.getPlain();
                    break;
                case ENCRYPTED:
                    // TODO: chech if this is right. Still confused whether decrypting here is ok, but there is not other way i suppose
                    if (encryptionInterface == null)
                        throw new IllegalStateException("no enc-interface");
                    s = encryptionInterface.decryptString(val.getEncrypted());
                    break;
                default:
                    continue;
            }
            uniq.add(s);
        }
        return uniq.size();
    }
}
    
