package nl.medtechchain.chaincode.service.encryption.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Request DTO for BFV homomorphic addition
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BfvAddAllRequest {
    private List<String> ciphertexts;
} 