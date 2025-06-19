package nl.medtechchain.chaincode.service.query;

import nl.medtechchain.chaincode.service.encryption.EncryptionService;

import java.util.List;
import java.util.Set;

// Test encryption service - just stores values as strings to make tests predictable
public class TestEncryptionService implements EncryptionService {
    
    private static final String DEFAULT_VERSION = "test-v1";
    private final boolean homomorphic;
    private final boolean supportsMultiplication;
    private final Set<String> availableVersions;
    private final String currentVersion;
    
    public TestEncryptionService() {
        this(true, false, Set.of(DEFAULT_VERSION), DEFAULT_VERSION);
    }
    
    public TestEncryptionService(boolean homomorphic, boolean supportsMultiplication) {
        this(homomorphic, supportsMultiplication, Set.of(DEFAULT_VERSION), DEFAULT_VERSION);
    }
    
    public TestEncryptionService(boolean homomorphic, boolean supportsMultiplication, 
                                Set<String> availableVersions, String currentVersion) {
        this.homomorphic = homomorphic;
        this.supportsMultiplication = supportsMultiplication;
        this.availableVersions = availableVersions;
        this.currentVersion = currentVersion;
    }
    
    @Override
    public String getCurrentVersion() {
        return currentVersion;
    }
    
    @Override
    public Set<String> getAvailableVersions() {
        return availableVersions;
    }
    
    @Override
    public long decryptLong(String ciphertext, String version) {
        // In test mode, "ciphertext" is just the plaintext value as string
        return Long.parseLong(ciphertext);
    }
    
    @Override
    public String decryptString(String ciphertext, String version) {
        // In test mode, "ciphertext" is just the plaintext value
        return ciphertext;
    }
    
    @Override
    public boolean decryptBool(String ciphertext, String version) {
        // In test mode, "ciphertext" is "1" for true, "0" for false
        return "1".equals(ciphertext);
    }
    
    @Override
    public boolean isHomomorphic() {
        return homomorphic;
    }
    
    @Override
    public boolean supportsMultiplication() {
        return supportsMultiplication;
    }
    
    @Override
    public String homomorphicAdd(List<String> ciphertexts, String version) {
        if (!homomorphic) {
            throw new UnsupportedOperationException("This test encryption service does not support homomorphic operations");
        }
        
        // Sum all the "encrypted" values (which are actually plaintext in our test)
        long sum = ciphertexts.stream()
                .mapToLong(Long::parseLong)
                .sum();
        
        return String.valueOf(sum);
    }
    
    @Override
    public String homomorphicMultiply(String ciphertext1, String ciphertext2, String version) {
        if (!supportsMultiplication) {
            throw new UnsupportedOperationException("This test encryption service does not support multiplication");
        }
        
        long value1 = Long.parseLong(ciphertext1);
        long value2 = Long.parseLong(ciphertext2);
        
        return String.valueOf(value1 * value2);
    }
    
    // helpers for generating test data
    
    public static TestDataGenerator.Ciphertext encryptLong(long value) {
        return encryptLong(value, DEFAULT_VERSION);
    }
    
    public static TestDataGenerator.Ciphertext encryptLong(long value, String version) {
        return new TestDataGenerator.Ciphertext(String.valueOf(value), version);
    }
    
    public static TestDataGenerator.Ciphertext encryptString(String value) {
        return encryptString(value, DEFAULT_VERSION);
    }
    
    public static TestDataGenerator.Ciphertext encryptString(String value, String version) {
        return new TestDataGenerator.Ciphertext(value, version);
    }
    
    // booleans are encoded as "1"/"0" strings
    public static TestDataGenerator.Ciphertext encryptBool(boolean value) {
        return encryptBool(value, DEFAULT_VERSION);
    }
    
    public static TestDataGenerator.Ciphertext encryptBool(boolean value, String version) {
        return new TestDataGenerator.Ciphertext(value ? "1" : "0", version);
    }

    @Override
    public String homomorphicSubWithScalar(String ciphertext, long scalar) {
        if (!supportsMultiplication) {
            throw new UnsupportedOperationException("This test encryption service does not support subtraction");
        }
        
        long value = Long.parseLong(ciphertext);
        return String.valueOf(value - scalar);
    }

    @Override
    public String homomorphicMultiplyWithScalar(String ciphertext, long scalar, String version) {
        if (!supportsMultiplication) {
            throw new UnsupportedOperationException("This test encryption service does not support multiplication");
        }
        
        long value = Long.parseLong(ciphertext);
        return String.valueOf(value * scalar);
    }
} 