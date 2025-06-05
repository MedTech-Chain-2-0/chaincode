package nl.medtechchain.chaincode.service.encryption.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response DTO for Paillier encryption key requests, includes version
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaillierEncryptionKeyResponse {
    private String encryptionKey;
    private String version;
} 