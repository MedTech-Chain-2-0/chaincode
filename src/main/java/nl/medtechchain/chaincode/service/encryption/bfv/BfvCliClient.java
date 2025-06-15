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
} 