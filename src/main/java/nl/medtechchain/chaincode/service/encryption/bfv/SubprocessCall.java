package nl.medtechchain.chaincode.service.encryption.bfv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;


public class SubprocessCall {

    private static final Logger logger = LoggerFactory.getLogger(SubprocessCall.class);

    public static String executeBfv(String cliBinary,
                                    String libraryDir,
                                    String... args) throws IOException {
        Path binPath = Path.of(cliBinary).toAbsolutePath();

        List<String> cmd = new ArrayList<>();
        cmd.add(binPath.toString());
        cmd.addAll(List.of(args));

        ProcessBuilder pb = new ProcessBuilder(cmd)
                .directory(Path.of(cliBinary).getParent().toFile())
                .redirectErrorStream(true);
//        pb.inheritIO(); // only if debugging

        pb.environment().put("LD_LIBRARY_PATH", libraryDir); // this shouldn't be hardcoded


        Process p = pb.start();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            String firstLine = br.readLine();
            int exit = p.waitFor();
            if (exit != 0)
                throw new RuntimeException("CLI exited with " + exit + " – cmd = " + String.join(" ", cmd));
            return firstLine;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("CLI interrupted", ie);
        }
    }
}
