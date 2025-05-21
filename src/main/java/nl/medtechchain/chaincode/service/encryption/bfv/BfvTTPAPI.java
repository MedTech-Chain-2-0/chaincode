package nl.medtechchain.chaincode.service.encryption.bfv;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import java.util.List;

public interface BfvTTPAPI {


    class CipherList {
        private List<String> ciphertexts;

        public CipherList() { }

        public CipherList(List<String> ciphertexts) {
            this.ciphertexts = ciphertexts;
        }

        public List<String> getCiphertexts() {
            return ciphertexts;
        }

        public void setCiphertexts(List<String> ciphertexts) {
            this.ciphertexts = ciphertexts;
        }
    }

    class CipherText {
        private String ciphertext;

        public CipherText() { }

        public CipherText(String ciphertext) {
            this.ciphertext = ciphertext;
        }

        public String getCiphertext() {
            return ciphertext;
        }

        public void setCiphertext(String ciphertext) {
            this.ciphertext = ciphertext;
        }

        @Override
        public String toString() {
            return ciphertext;
        }
    }

    class PlainText {
        private String plaintext;
        public PlainText() {}
        public PlainText(String plaintext){this.plaintext = plaintext;}
        public String getPlaintext(){return plaintext;}
        public void setPlaintext(String plaintext){this.plaintext = plaintext;}
    }

    CipherText encrypt(long plaintext) throws IOException, InterruptedException;

    String addAll(List<String> cts) throws IOException, InterruptedException;

    long decrypt(String ct) throws IOException, InterruptedException;

    static BfvTTPAPI getInstance(String ttpAddress) {
        return new BfvTTPAPI() {
            private final ObjectMapper om     = new ObjectMapper();
            private final HttpClient   client = HttpClient.newHttpClient();

            private <T> T postJson(String path, Object body, Class<T> cls)
                    throws IOException, InterruptedException {

                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create("http://" + ttpAddress + path))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(
                                om.writeValueAsString(body)))
                        .build();

                HttpResponse<String> res =
                        client.send(req, HttpResponse.BodyHandlers.ofString());

                return om.readValue(res.body(), cls);
            }


            @Override
            public CipherText encrypt(long m)
                    throws IOException, InterruptedException {

                class EncryptRequest {
                    public long plaintext;
                    EncryptRequest(long plaintext) { this.plaintext = plaintext; }
                }

                return postJson("/api/bfv/encrypt",
                        new EncryptRequest(m),
                        CipherText.class);
            }

            @Override
            public String addAll(List<String> cts)
                    throws IOException, InterruptedException {

                return postJson("/api/bfv/addAll",
                        new CipherList(cts),
                        CipherText.class)
                        .getCiphertext();
            }

            @Override
            public long decrypt(String ct) throws IOException, InterruptedException {
                class DecryptReq { public String ciphertext; DecryptReq(String c){this.ciphertext=c;} }
                PlainText pt = postJson("/api/bfv/decrypt", new DecryptReq(ct), PlainText.class);
                return Long.parseLong(pt.getPlaintext());
            }
        };
    }
}
