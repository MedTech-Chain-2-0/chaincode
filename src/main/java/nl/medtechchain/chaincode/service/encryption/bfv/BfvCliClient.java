package nl.medtechchain.chaincode.service.encryption.bfv;

import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

@Component
public class BfvCliClient {
    private final BfvCliProperties properties;

    public BfvCliClient(BfvCliProperties properties) {
        this.properties = properties;
    }


    public String addMany(List<String> cts) throws IOException {
        if (!properties.isEnabled()) {
            throw new UnsupportedOperationException("BFV functionality is disabled. Set bfv.cli.enabled=true to enable.");
        }
        if (cts == null || cts.isEmpty())
            throw new IllegalArgumentException("Ciphertext list must not be empty");

        List<String> args = new ArrayList<>(cts.size() + 1);
        args.add("addMany");
        args.addAll(cts);

        return SubprocessCall.executeBfv(
                properties.getBinary(), properties.getLibraryPath(),
                args.toArray(String[]::new)).trim();
    }
}
