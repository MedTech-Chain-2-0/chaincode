package nl.medtechchain.chaincode.service.encryption.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaillierDecryptRequest {
    private String encryptionKey;
    private String ciphertext;
    private String version;
    
    public PaillierDecryptRequest(String encryptionKey, String ciphertext) {
        this.encryptionKey = encryptionKey;
        this.ciphertext = ciphertext;
        this.version = null;
    }
} 