package nl.medtechchain.chaincode.service.encryption;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.medtechchain.chaincode.service.encryption.dto.*;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

// BFV TTP API - encrypt, decrypt, add homomorphically
public interface BfvTTPAPI {
    
    String encrypt(long plaintext) throws IOException, InterruptedException;
    
    // add multiple encrypted values homomorphically
    String addAll(List<String> ciphertexts) throws IOException, InterruptedException;
    
    long decrypt(String ciphertext) throws IOException, InterruptedException;
    
    static BfvTTPAPI getInstance(String ttpAddress) {
        return new BfvTTPAPI() {
            private final ObjectMapper om = new ObjectMapper();
            private final HttpClient httpClient = HttpClient.newHttpClient();
            
            @Override
            public String encrypt(long plaintext) throws IOException, InterruptedException {
                var requestBody = om.writeValueAsString(new BfvEncryptRequest(plaintext));
                var request = HttpRequest.newBuilder()
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                        .uri(URI.create("http://" + ttpAddress + "/api/bfv/encrypt"))
                        .build();
                
                var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                var encryptResponse = om.readValue(response.body(), BfvEncryptResponse.class);
                return encryptResponse.getCiphertext();
            }
            
            @Override
            public String addAll(List<String> ciphertexts) throws IOException, InterruptedException {
                var requestBody = om.writeValueAsString(new BfvAddAllRequest(ciphertexts));
                var request = HttpRequest.newBuilder()
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                        .uri(URI.create("http://" + ttpAddress + "/api/bfv/addAll"))
                        .build();
                
                var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                var addResponse = om.readValue(response.body(), BfvAddAllResponse.class);
                return addResponse.getSumCiphertext();
            }
            
            @Override
            public long decrypt(String ciphertext) throws IOException, InterruptedException {
                var requestBody = om.writeValueAsString(new BfvDecryptRequest(ciphertext));
                var request = HttpRequest.newBuilder()
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                        .uri(URI.create("http://" + ttpAddress + "/api/bfv/decrypt"))
                        .build();
                
                var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                var decryptResponse = om.readValue(response.body(), BfvDecryptResponse.class);
                return Long.parseLong(decryptResponse.getPlaintext());
            }
        };
    }
} 