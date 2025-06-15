package nl.medtechchain.chaincode.service.encryption.bfv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/** Utility to run the BFV calc binary and grab its first stdout line. */
class SubprocessCall {

    private static final Logger logger = Logger.getLogger(SubprocessCall.class.getName());

    private SubprocessCall() {
        // utility class
    }

    /** Run binary and return trimmed stdout. */
    static String executeBfv(String cliBinary, String... args) throws IOException {
        Path binPath = Path.of(cliBinary).toAbsolutePath();

        List<String> cmd = new ArrayList<>(args.length + 1);
        cmd.add(binPath.toString());
        cmd.addAll(List.of(args));

        ProcessBuilder pb = new ProcessBuilder(cmd)
                .directory(binPath.getParent().toFile())
                .redirectErrorStream(true);

        logger.fine("Executing CLI: " + String.join(" ", cmd));

        Process proc = pb.start();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
            String firstLine = br.readLine();
            int exit = proc.waitFor();
            if (exit != 0) {
                throw new IOException("CLI exited with code " + exit + ": " + String.join(" ", cmd));
            }
            return firstLine == null ? "" : firstLine;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("CLI interrupted", ie);
        }
    }
} 