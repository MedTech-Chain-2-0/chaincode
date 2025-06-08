package nl.medtechchain.chaincode.service.encryption.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response DTO for BFV decryption
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BfvDecryptResponse {
    private String plaintext;
} 