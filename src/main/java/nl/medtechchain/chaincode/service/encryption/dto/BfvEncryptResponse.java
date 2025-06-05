package nl.medtechchain.chaincode.service.encryption.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response DTO for BFV encryption
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BfvEncryptResponse {
    private String ciphertext;
} 