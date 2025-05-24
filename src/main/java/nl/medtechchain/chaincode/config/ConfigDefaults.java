package nl.medtechchain.chaincode.config;

import com.google.protobuf.Timestamp;
import nl.medtechchain.chaincode.service.encryption.paillier.PaillierTTPAPI;
import nl.medtechchain.proto.config.NetworkConfig;
import nl.medtechchain.proto.config.PlatformConfig;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import static nl.medtechchain.proto.config.NetworkConfig.HospitalConfig.Config.CONFIG_HOSPITAL_APPLICATION_SERVER_ADDRESS;
import static nl.medtechchain.proto.config.PlatformConfig.Config.*;

public class ConfigDefaults {
    private static final Logger logger = Logger.getLogger(ConfigDefaults.class.getName());

    private ConfigDefaults() {
    }

    public static class NetworkDefaults {
        public static NetworkConfig defaultNetworkConfig() {
            var healpoint = NetworkConfig.HospitalConfig
                    .newBuilder()
                    .setName("HealPoint")
                    .addAllMap(List.of(
                            entry(CONFIG_HOSPITAL_APPLICATION_SERVER_ADDRESS, "http://hospital.healpoint.nl:8080")
                    ))
                    .build();

            var lifecare = NetworkConfig.HospitalConfig
                    .newBuilder()
                    .setName("LifeCare")
                    .addAllMap(List.of(
                            entry(CONFIG_HOSPITAL_APPLICATION_SERVER_ADDRESS, "http://hospital.lifecare.nl:8080")
                    ))
                    .build();

            return NetworkConfig.newBuilder()
                    .setTimestamp(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                    .addAllList(List.of(healpoint, lifecare))
                    .build();
        }

        private static NetworkConfig.HospitalConfig.Entry entry(NetworkConfig.HospitalConfig.Config key, String value) {
            return NetworkConfig.HospitalConfig.Entry.newBuilder().setKey(key).setValue(value).build();
        }
    }

    public static class PlatformConfigDefaults {

        public static class EncryptionDefaults {

             /*
             Note regarding TTP ADDRESS: currently the chaincode is setup to contact the ttp within
             the same docker network. If you want to test the bfv scheme, you have to run ttp locally (from within wsl)
             and change the below address to the output of the following command: "ip -4 addr show eth0 | grep -oP '(?<=inet\s)\d+(\.\d+){3}'". 
             Port should stay the same 
             */
            public static final String TTP_ADDRESS = "ttp.medtechchain.nl:6000"; //"192.168.45.84:6000"; 
           
            public static final int BIT_LENGTH = 2048;
        }

        public static List<PlatformConfig.Entry> defaultPlatformConfigs() {
            var list = new ArrayList<PlatformConfig.Entry>();

            list.add(entry(CONFIG_FEATURE_QUERY_INTERFACE_COUNT_FIELDS, "udi,hospital,manufacturer,model,firmware_version,device_type,category,speciality"));
            list.add(entry(CONFIG_FEATURE_QUERY_INTERFACE_GROUPED_COUNT_FIELDS, "hospital,manufacturer,model,device_type,category,speciality"));
            list.add(entry(CONFIG_FEATURE_QUERY_INTERFACE_AVERAGE_FIELDS, "production_date,warranty_expiry_date,usage_hours"));
            list.add(entry(CONFIG_FEATURE_QUERY_INTERFACE_SUM_FIELDS, "usage_hours"));
            list.add(entry(CONFIG_FEATURE_QUERY_INTERFACE_UNIQUE_COUNT_FIELDS, "hospital,manufacturer,model,firmware_version,device_type,category,speciality"));
            list.add(entry(CONFIG_FEATURE_QUERY_INTERFACE_HISTOGRAM_FIELDS, "usage_hours,battery_level, production_date, last_service_date,warranty_expiry_date"));

            list.add(entry(CONFIG_FEATURE_QUERY_DIFFERENTIAL_PRIVACY, "laplace"));
            list.add(entry(CONFIG_FEATURE_QUERY_DIFFERENTIAL_PRIVACY_LAPLACE_EPSILON, "1"));
            list.add(entry(CONFIG_FEATURE_QUERY_ENCRYPTION_PAILLIER_BIT_LENGTH, EncryptionDefaults.BIT_LENGTH + ""));
            list.add(entry(CONFIG_FEATURE_QUERY_ENCRYPTION_PAILLIER_TTP_ADRRESS, EncryptionDefaults.TTP_ADDRESS));
            list.add(entry(CONFIG_FEATURE_AUDITING_KEY_EXCHANGE_ENABLED, "false"));

            // ===== Encryption Scheme Configuration =====
            // Choose one of the following encryption schemes:
            // 1. "bfv" - Brakerski-Fan-Vercauteren scheme for homomorphic encryption
            //    - Fully homomorphic, big ciphertext size
            //    - Requires TTP service running
            // 2. "paillier" - Paillier homomorphic encryption
            //    - Simpler
            //    - Also requires TTP service
            // 3. "none" - No encryption
            //    - Use for testing or when encryption is not needed
            
            // Uncomment to use BFV instead
            // list.add(entry(CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME, "bfv"));
            
            // Uncomment to use Paillier instead
            // list.add(entry(CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME, "paillier"));
            
            // Uncomment to disable encryption
            list.add(entry(CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME, "none"));

            return list;
        }

        private static PlatformConfig.Entry entry(PlatformConfig.Config key, String value) {
            return PlatformConfig.Entry.newBuilder().setKey(key).setValue(value).build();
        }
    }
}
