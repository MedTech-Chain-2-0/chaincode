package nl.medtechchain.chaincode.test;

import nl.medtechchain.chaincode.service.encryption.PlatformEncryptionInterface;

public class DummyEncryption implements PlatformEncryptionInterface {
    @Override 
    public String encryptString(String plaintext) { 
        return plaintext; 
    }

    @Override 
    public String encryptLong(long plaintext){
        return Long.toString(plaintext); 
    }

    @Override 
    public String encryptBool(boolean plaintext){ 
        return plaintext ? "1" : "0"; 
    }

    @Override 
    public String decryptString(String cyphertext){ 
        return cyphertext; 
    }

    @Override 
    public long decryptLong(String cyphertext)  { 
        return Long.parseLong(cyphertext); 
    }

    @Override 
    public boolean decryptBool(String cyphertext) { 
        return cyphertext.equals("1"); 
    }
}
