package nl.medtechchain.chaincode.service.encryption.bfv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
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

        // For bfv_calc, we need to pass ciphertexts via stdin
        // The first arg is the command (e.g., "addMany"), the rest are ciphertexts
        List<String> cmd = new ArrayList<>();
        cmd.add(binPath.toString());
        if (args.length > 0) {
            cmd.add(args[0]); // Add the command (addMany, mul, etc.)
        }

        ProcessBuilder pb = new ProcessBuilder(cmd)
                .directory(binPath.getParent().toFile())
                .redirectErrorStream(true);

        logger.fine("Executing CLI: " + String.join(" ", cmd));

        Process proc = pb.start();
        
        // Write ciphertexts to stdin if we have any (skip the first arg which is the command)
        if (args.length > 1) {
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(proc.getOutputStream()))) {
                for (int i = 1; i < args.length; i++) {
                    writer.write(args[i]);
                    writer.newLine();
                }
                writer.flush();
            }
        }
        
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