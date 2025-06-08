package nl.medtechchain.chaincode.service.encryption.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response DTO for Paillier encryption, includes version
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaillierEncryptResponse {
    private String ciphertext;
    private String version;
} 