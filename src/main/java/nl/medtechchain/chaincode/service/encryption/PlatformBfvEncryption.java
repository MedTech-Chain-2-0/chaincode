package nl.medtechchain.chaincode.service.encryption;

import nl.medtechchain.chaincode.service.encryption.bfv.BfvTTPAPI;

import java.util.List;
import java.util.concurrent.Callable;

/**
   Platform-level wrapper for BFV homomorphic encryption operations.
   Provides a simplified interface for encrypting/decrypting values and performing
   homomorphic additions, while delegating the actual cryptographic operations to the TTP service.
 */
public class PlatformBfvEncryption
        implements PlatformEncryptionInterface, HomomorphicEncryptionScheme {

    private final BfvTTPAPI api;
    PlatformBfvEncryption(String ttpAddr) { this.api = BfvTTPAPI.getInstance(ttpAddr); }

    public String encryptLong(long p) { return sneaky(() -> api.encrypt(p).getCiphertext()); }
    public long   decryptLong(String c) { return sneakyLong(() -> api.decrypt(c)); }

    public String encryptString(String s){ throw new UnsupportedOperationException(); }
    public String encryptBool(boolean b){ throw new UnsupportedOperationException(); }
    public String decryptString(String c){ throw new UnsupportedOperationException(); }
    public boolean decryptBool(String c){ throw new UnsupportedOperationException(); }

    /* ------------- homomorphic ops ------------------------------ */
    public String add(String c1,String c2){ return sneaky(() -> api.addAll(List.of(c1,c2))); }
    public String addAll(List<String> list){ return sneaky(() -> api.addAll(list)); }

    public String mulCt(String ct,String c){ throw new UnsupportedOperationException(); }
    public String mul(String c1,String c2){ throw new UnsupportedOperationException(); }

    private <T> T sneaky(java.util.concurrent.Callable<T> c){
        try{ return c.call(); }catch(Exception e){ throw new RuntimeException(e);}
    }

    private long sneakyLong(java.util.concurrent.Callable<Long> c){
        try{ return c.call(); }catch(Exception e){ throw new RuntimeException(e);}
    }
}
