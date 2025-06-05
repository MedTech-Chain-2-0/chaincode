package nl.medtechchain.chaincode.service.encryption;

import nl.medtechchain.chaincode.service.encryption.dto.PaillierDecryptRequest;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

// Paillier encryption implementation - talks to TTP for key management  
// Can do homomorphic addition but not multiplication
public class PaillierEncryptionService implements EncryptionService {
    
    private static final Logger logger = Logger.getLogger(PaillierEncryptionService.class.getName());
    
    private final PaillierTTPAPI api;
    
    public PaillierEncryptionService(String ttpAddress) {
        this.api = PaillierTTPAPI.getInstance(ttpAddress);
    }
    
    @Override
    public String getCurrentVersion() {
        try {
            return api.encryptionKey(2048).getVersion();
        } catch (Exception e) {
            logger.severe("Failed to get current version from TTP: " + e.getMessage());
            throw new RuntimeException("Failed to get current version", e);
        }
    }
    
    @Override
    public Set<String> getAvailableVersions() {
        // TTP manages versions, we don't track them locally
        return Set.of(getCurrentVersion());
    }
    
    // Decryption methods
    
    @Override
    public long decryptLong(String ciphertext, String version) {
        BigInteger result = decrypt(ciphertext, version);
        return result.longValue();
    }
    
    @Override
    public String decryptString(String ciphertext, String version) {
        BigInteger result = decrypt(ciphertext, version);
        return bigIntegerToString(result);
    }
    
    @Override
    public boolean decryptBool(String ciphertext, String version) {
        BigInteger result = decrypt(ciphertext, version);
        return result.equals(BigInteger.ONE);
    }
    
    // Homomorphic operations
    
    @Override
    public boolean isHomomorphic() {
        return true; // Paillier supports homomorphic addition
    }
    
    @Override
    public boolean supportsMultiplication() {
        return false; // Paillier does not support homomorphic multiplication
    }
    
    @Override
    public String homomorphicAdd(List<String> ciphertexts, String version) {
        if (ciphertexts == null || ciphertexts.isEmpty()) {
            throw new IllegalArgumentException("Ciphertext list cannot be null or empty");
        }
        
        // In Paillier, homomorphic addition is done via multiplication of ciphertexts
        BigInteger result = new BigInteger(ciphertexts.get(0));
        
        for (int i = 1; i < ciphertexts.size(); i++) {
            result = result.multiply(new BigInteger(ciphertexts.get(i)));
        }
        
        return result.toString();
    }
    
    @Override
    public String homomorphicMultiply(String ciphertext1, String ciphertext2, String version) {
        throw new UnsupportedOperationException(
            "Homomorphic multiplication is not supported by Paillier encryption. " +
            "Use BFV encryption for multiplication operations."
        );
    }
    
    // helper methods
    
    private BigInteger decrypt(String ciphertext, String version) {
        try {
            // Let TTP handle key lookup by version - we don't manage keys locally
            PaillierDecryptRequest request = new PaillierDecryptRequest(null, ciphertext, version);
            String plaintext = api.decrypt(request).getPlaintext();
            return new BigInteger(plaintext);
        } catch (Exception e) {
            logger.severe("TTP decryption failed for version " + version + ": " + e.getMessage());
            throw new RuntimeException("TTP decryption failed", e);
        }
    }
    
    private String bigIntegerToString(BigInteger bigInt) {
        byte[] bytes = bigInt.toByteArray();
        
        // Remove leading zero byte if present (sign bit)
        if (bytes.length > 0 && bytes[0] == 0) {
            byte[] temp = new byte[bytes.length - 1];
            System.arraycopy(bytes, 1, temp, 0, temp.length);
            bytes = temp;
        }
        
        return new String(bytes, StandardCharsets.UTF_8);
    }
}