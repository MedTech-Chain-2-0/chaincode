package nl.medtechchain.chaincode.service.encryption;

import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import nl.medtechchain.chaincode.service.encryption.bfv.BfvCliClient;
import nl.medtechchain.chaincode.service.encryption.BfvTTPAPI;

// BFV encryption: ints only, homomorphic add.
public class BfvEncryptionService implements EncryptionService {
    
    private static final Logger logger = Logger.getLogger(BfvEncryptionService.class.getName());
    private static final String BFV_VERSION = "bfv-default";
    
    private final BfvCliClient cli;
    private final BfvTTPAPI api;
    
    public BfvEncryptionService(String cliBinaryPath, String ttpAddress) {
        this.cli = new BfvCliClient(cliBinaryPath);
        this.api = BfvTTPAPI.getInstance(ttpAddress);
    }
    
    @Override
    public String getCurrentVersion() {
        // BFV doesn't use versioned keys in the current TTP implementation
        return BFV_VERSION;
    }
    
    @Override
    public Set<String> getAvailableVersions() {
        return Set.of(BFV_VERSION);
    }
    
    // decrypt operations
    
    @Override
    public long decryptLong(String ciphertext, String version) {
        try {
            return api.decrypt(ciphertext);
        } catch (Exception e) {
            logger.severe("BFV decryption failed: " + e.getMessage());
            throw new RuntimeException("BFV decryption failed", e);
        }
    }
    
    @Override
    public String decryptString(String ciphertext, String version) {
        throw new UnsupportedOperationException(
            "BFV encryption does not support string decryption. " +
            "Only long integer operations are supported."
        );
    }
    
    @Override
    public boolean decryptBool(String ciphertext, String version) {
        throw new UnsupportedOperationException(
            "BFV encryption does not support boolean decryption. " +
            "Only long integer operations are supported."
        );
    }
    
    // homomorphic stuff
    
    @Override
    public boolean isHomomorphic() {
        return true; // BFV supports homomorphic operations
    }
    
    @Override
    public boolean supportsMultiplication() {
        // Current TTP BFV implementation doesn't expose multiplication endpoint
        return false;
    }
    
    @Override
    public String homomorphicAdd(List<String> ciphertexts, String version) {
        if (ciphertexts == null || ciphertexts.isEmpty()) {
            throw new IllegalArgumentException("Ciphertext list cannot be null or empty");
        }
        
        if (ciphertexts.size() == 1) {
            return ciphertexts.get(0);
        }
        
        try {
            return cli.addMany(ciphertexts);
        } catch (Exception e) {
            logger.severe("BFV homomorphic addition failed: " + e.getMessage());
            throw new RuntimeException("BFV homomorphic addition failed", e);
        }
    }
    
    @Override
    public String homomorphicMultiply(String ciphertext1, String ciphertext2, String version) {
        if (ciphertext1 == null || ciphertext2 == null) {
            throw new IllegalArgumentException("Ciphertexts cannot be null");
        }
        
        
        try {
            return cli.multiply(ciphertext1, ciphertext2);
        } catch (Exception e) {
            logger.severe("BFV homomorphic multiplication failed: " + e.getMessage());
            throw new RuntimeException("BFV homomorphic multiplication failed", e);
        }
    }
} 