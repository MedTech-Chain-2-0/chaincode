package nl.medtechchain.chaincode.service.encryption.bfv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Minimal wrapper around the bfv_calc binary. */
public class BfvCliClient {

    private final String binaryPath;

    public BfvCliClient(String binaryPath) {
        if (binaryPath == null || binaryPath.isBlank())
            throw new IllegalArgumentException("cli path missing");
        this.binaryPath = binaryPath;
    }

    public String addMany(List<String> ciphertexts) throws IOException {
        if (ciphertexts == null || ciphertexts.isEmpty())
            throw new IllegalArgumentException("ciphertexts must not be empty");
        List<String> args = new ArrayList<>(ciphertexts.size() + 1);
        args.add("addMany");
        args.addAll(ciphertexts);
        return SubprocessCall.executeBfv(binaryPath, args.toArray(String[]::new)).trim();
    }

    
    public String multiply(String ciphertext1, String ciphertext2) throws IOException {
        if (ciphertext1 == null || ciphertext2 == null) {
            throw new IllegalArgumentException("Ciphertexts cannot be null");
        }   
        List<String> args = new ArrayList<>(3);
        args.add("mul");
        args.add(ciphertext1);
        args.add(ciphertext2);
        return SubprocessCall.executeBfv(binaryPath, args.toArray(String[]::new)).trim();
    }

    public String subtractScalar(String ciphertext, long scalar) throws IOException {
        if (ciphertext == null) {
            throw new IllegalArgumentException("Ciphertext cannot be null");
        }   
        List<String> args = new ArrayList<>(3);
        args.add("subtractScalar");
        args.add(ciphertext);
        args.add(Long.toString(scalar));
        return SubprocessCall.executeBfv(binaryPath, args.toArray(String[]::new)).trim();
    }    
} 