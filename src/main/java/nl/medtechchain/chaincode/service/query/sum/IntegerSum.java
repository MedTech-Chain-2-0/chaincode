package nl.medtechchain.chaincode.service.query.sum;

import com.google.protobuf.Descriptors;
import nl.medtechchain.chaincode.service.encryption.HomomorphicEncryptionScheme;
import nl.medtechchain.chaincode.service.encryption.PlatformBfvEncryption;
import nl.medtechchain.chaincode.service.encryption.PlatformEncryptionInterface;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;

import java.util.ArrayList;
import java.util.List;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;


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

        // ---- DEBUG: ping TTP /testt endpoint to prove reachability ----
        // try {
        //     String adr = nl.medtechchain.chaincode.config.ConfigDefaults.PlatformConfigDefaults.EncryptionDefaults.TTP_ADDRESS;
        //     HttpClient.newHttpClient()
        //             .send(HttpRequest.newBuilder()
        //                     .uri(URI.create("http://" + adr + "/api/bfv/testt"))
        //                     .GET()
        //                     .build(),
        //                   HttpResponse.BodyHandlers.discarding());
        // } catch (Exception e) {
        //     throw new IllegalStateException("EXCEPTION IN TTP REQUEST: " + e.getClass().getName() + ": " + e.getMessage(), e);
        //  }
        // --------------------------------------------------------------

        long plainSum = 0;
        List<String> encrypted = new ArrayList<>();

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
                        encrypted.add(fieldValue.getEncrypted());
                    } else {
                        // no homomorphic encryption -> we have the decrypt
                        // done like this in other queries, but not sure if it should be done like this
                        // it works tho :)
                        plainSum += encryptionInterface.decryptLong(fieldValue.getEncrypted());
                    }
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
            else {                                // fallback – pairwise
                ctSum = encrypted.get(0);
                for (int i = 1; i < encrypted.size(); i++)
                    ctSum = ((HomomorphicEncryptionScheme)encryptionInterface).add(ctSum, encrypted.get(i));
            }
            plainSum += encryptionInterface.decryptLong(ctSum);
        }
        return plainSum;    }
}
