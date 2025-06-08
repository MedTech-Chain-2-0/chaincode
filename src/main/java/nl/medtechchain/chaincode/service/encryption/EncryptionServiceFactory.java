package nl.medtechchain.chaincode.service.encryption;

import nl.medtechchain.proto.config.PlatformConfig;

import java.util.Optional;
import java.util.logging.Logger;

import static nl.medtechchain.chaincode.config.ConfigOps.PlatformConfigOps.get;
import static nl.medtechchain.chaincode.config.ConfigOps.PlatformConfigOps.getUnsafe;
import static nl.medtechchain.proto.config.PlatformConfig.Config.*;

// Creates encryption services from platform config
public class EncryptionServiceFactory {
    
    private static final Logger logger = Logger.getLogger(EncryptionServiceFactory.class.getName());
    
    public static EncryptionService create(PlatformConfig config) {
        Optional<String> encryptionMethod = get(config, CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME);
        
        if (encryptionMethod.isEmpty()) {
            logger.info("No encryption configured");
            return null;
        }
        
        switch (encryptionMethod.get().toLowerCase()) {
            case "paillier":
                String ttpAddress = getUnsafe(config, CONFIG_FEATURE_QUERY_ENCRYPTION_PAILLIER_TTP_ADRRESS);
                logger.info("Creating Paillier encryption service with TTP: " + ttpAddress);
                return new PaillierEncryptionService(ttpAddress);
                
            case "bfv":
                // BFV uses the same TTP address config key as Paillier currently
                String bfvTtpAddress = getUnsafe(config, CONFIG_FEATURE_QUERY_ENCRYPTION_PAILLIER_TTP_ADRRESS);
                logger.info("Creating BFV encryption service with TTP: " + bfvTtpAddress);
                return new BfvEncryptionService(bfvTtpAddress);
                
            case "plaintext":
            case "none":
                logger.info("No encryption configured");
                return null;
                
            default:
                logger.warning("Unknown encryption method: " + encryptionMethod.get() + ", no encryption");
                return null;
        }
    }
} 