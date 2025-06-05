package nl.medtechchain.chaincode.service.encryption.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response DTO for BFV homomorphic addition
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BfvAddAllResponse {
    private String sumCiphertext;
} 