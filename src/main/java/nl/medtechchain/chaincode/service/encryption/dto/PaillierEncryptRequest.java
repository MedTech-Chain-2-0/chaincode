package nl.medtechchain.chaincode.service.encryption.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request DTO for Paillier encryption
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaillierEncryptRequest {
    private String plaintext;
    private String encryptionKey;
} 