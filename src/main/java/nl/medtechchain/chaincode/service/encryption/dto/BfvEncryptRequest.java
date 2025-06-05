package nl.medtechchain.chaincode.service.encryption.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request DTO for BFV encryption
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BfvEncryptRequest {
    private long plaintext;
} 