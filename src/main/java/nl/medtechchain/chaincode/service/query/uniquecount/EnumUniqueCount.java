package nl.medtechchain.chaincode.service.query.uniquecount;
import nl.medtechchain.chaincode.service.encryption.PlatformEncryptionInterface;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.protobuf.Descriptors;

public class EnumUniqueCount implements UniqueCount {
    @Override
    public long uniqueCount(PlatformEncryptionInterface encryptionInterface,
                            Descriptors.FieldDescriptor descriptor,
                            List<DeviceDataAsset> assets) {
        Set<Integer> uniq = new HashSet<>();
        for (DeviceDataAsset asset: assets) {
            var field = asset.getDeviceData().getField(descriptor);

            // only two fields match the enum category
            if(field instanceof DeviceDataAsset.DeviceCategoryField){
                DeviceDataAsset.DeviceCategoryField fieldCasted = ((DeviceDataAsset.DeviceCategoryField) field);

                switch (fieldCasted.getFieldCase()) {
                    case PLAIN:
                        uniq.add(fieldCasted.getPlain().getNumber());
                        break;
                    case ENCRYPTED:
                        if (encryptionInterface == null)
                            throw new IllegalStateException("no enc-interface");
                        uniq.add((int)encryptionInterface.decryptLong(fieldCasted.getEncrypted()));
                        break;
                    default:
                        // not matching
                        break;
                }
            }
            else if(field instanceof DeviceDataAsset.MedicalSpecialityField){
                DeviceDataAsset.MedicalSpecialityField fieldCasted = ((DeviceDataAsset.MedicalSpecialityField) field);

                switch (fieldCasted.getFieldCase()) {
                    case PLAIN:
                        uniq.add(fieldCasted.getPlain().getNumber());
                        break;
                    case ENCRYPTED:
                        if (encryptionInterface == null)
                            throw new IllegalStateException("no enc-interface");
                        uniq.add((int)encryptionInterface.decryptLong(fieldCasted.getEncrypted()));
                        break;
                    default:
                        // not matching
                        break;
                }
            }
        }
        return uniq.size();
    }
}
