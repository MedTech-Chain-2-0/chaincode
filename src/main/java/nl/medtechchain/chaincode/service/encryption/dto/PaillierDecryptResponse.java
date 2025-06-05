package nl.medtechchain.chaincode.service.encryption.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response DTO for Paillier decryption
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaillierDecryptResponse {
    private String plaintext;
} 