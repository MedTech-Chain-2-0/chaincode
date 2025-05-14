package nl.medtechchain.chaincode.service.query.sum;

import com.google.protobuf.Descriptors;
import nl.medtechchain.chaincode.service.encryption.HomomorphicEncryptionScheme;
import nl.medtechchain.chaincode.service.encryption.PlatformEncryptionInterface;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import java.util.List;


class IntegerSum implements Sum {
    /*  
    * Performs the actual calculations of the sum. If in plain it just sums them for every asset,
    if we are in encrypted mode it uses the interface operations
    @param encryptionInterface - encryption interface used for decryption and homomorphic operations, pluggable, specified elsewhere
    @param descriptor - descriptor specifying which field to use, complicated thingy needed because we use protobuffs
    @param assets - list of device data assets to sum over
    @return sum - of the specified field of all assets
    */
    @Override
    public long sum(PlatformEncryptionInterface encryptionInterface, Descriptors.FieldDescriptor descriptor,
                    List<DeviceDataAsset> assets) {

        long plainSum = 0;
        String homoSum = null;

        for (DeviceDataAsset a : assets) {
            // very complicated getter for a field, made like this because we working with protobuffs
            // done like this in other queries
            var fieldValue = (DeviceDataAsset.IntegerField) a.getDeviceData().getField(descriptor);
            switch (fieldValue.getFieldCase()) {
                case PLAIN:
                    plainSum += fieldValue.getPlain();
                    break;
                case ENCRYPTED:
                    if (encryptionInterface == null)
                        throw new IllegalStateException("Field " + descriptor.getName() + " is encrypted, but the platform is not properly configured to use encryption.");

                    if (encryptionInterface.isHomomorphic()) {
                        // ATENTION
                        // not exactly sure that it should be done like this
                        // might depent on the schema
                        // done like this in other queries
                        homoSum = (homoSum == null)
                                ? fieldValue.getEncrypted()
                                : ((HomomorphicEncryptionScheme) encryptionInterface).add(homoSum, fieldValue.getEncrypted());
                    } else {
                        // no homomorphic encryption -> we have the decrypt
                        // done like this in other queries, but not sure if it should be done like this
                        // it works tho :)
                        plainSum += encryptionInterface.decryptLong(fieldValue.getEncrypted());
                    }
                    break;
            }
        }
        if (homoSum != null) plainSum += encryptionInterface.decryptLong(homoSum);
        return plainSum;
    }
}