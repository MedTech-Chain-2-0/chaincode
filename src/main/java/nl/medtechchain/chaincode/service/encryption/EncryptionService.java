package nl.medtechchain.chaincode.service.encryption;

import java.util.List;
import java.util.Set;

// Common interface for encryption schemes - handles Paillier, BFV, or plaintext
public interface EncryptionService {
    
    // Version stuff
    
    String getCurrentVersion();
    
    Set<String> getAvailableVersions();
    
    // Basic decryption methods
    
    long decryptLong(String ciphertext, String version);
    
    String decryptString(String ciphertext, String version);
    
    // decrypt boolean values
    boolean decryptBool(String ciphertext, String version);
    
    // Homomorphic capabilities
    
    // does this encryption support homomorphic ops?
    boolean isHomomorphic();
    
    boolean supportsMultiplication();
    
    // add encrypted values together without decrypting
    String homomorphicAdd(List<String> ciphertexts, String version);
    
    // multiply two encrypted values (only works with some schemes)
    String homomorphicMultiply(String ciphertext1, String ciphertext2, String version);

    String homomorphicSubWithScalar(String ciphertext, long scalar);

    String homomorphicMultiplyWithScalar(String ciphertext, long scalar, String version);
} 