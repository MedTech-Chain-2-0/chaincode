package nl.medtechchain.chaincode.service.encryption.bfv;

import jakarta.annotation.PostConstruct;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Component
@Data
@ConfigurationProperties(prefix = "bfv.cli")
public class BfvCliProperties {
    /** Absolute path to the bfv_client executable */
    private String binary = Paths.get("binaries", "bfv_cli").toAbsolutePath().toString();
    /** Directory that must be present on LD_LIBRARY_PATH (no default!). */
    private String libraryPath;
    /** Whether to enable BFV functionality (default: false) */
    private boolean enabled = false;

    /** Validate early so the service fails fast with a clear message. */
    @PostConstruct
    void validate() {
        if (!enabled) {
            return; // Skip validation if BFV is disabled
        }

        Path bin  = Path.of(binary);
        Path libs = Path.of(libraryPath);

        if (!Files.isExecutable(bin))
            throw new IllegalStateException("bfv.cli.binary does not point to an executable: " + bin);

        if (!Files.isDirectory(libs))
            throw new IllegalStateException("bfv.cli.library-path is not a directory: " + libs);
    }
}