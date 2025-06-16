package nl.medtechchain.chaincode.contract;

import com.google.protobuf.InvalidProtocolBufferException;
import nl.medtechchain.chaincode.config.ConfigOps;
import nl.medtechchain.proto.config.NetworkConfig;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.config.UpdateNetworkConfig;
import nl.medtechchain.proto.config.UpdatePlatformConfig;
import org.hyperledger.fabric.Logger;
import org.hyperledger.fabric.contract.Context;
import org.hyperledger.fabric.contract.ContractInterface;
import org.hyperledger.fabric.contract.annotation.Contract;
import org.hyperledger.fabric.contract.annotation.Info;
import org.hyperledger.fabric.contract.annotation.License;
import org.hyperledger.fabric.contract.annotation.Transaction;

import static nl.medtechchain.chaincode.config.ConfigDefaults.NetworkDefaults;
import static nl.medtechchain.chaincode.config.ConfigDefaults.PlatformConfigDefaults;
import static nl.medtechchain.chaincode.util.Base64EncodingOps.decode64;
import static nl.medtechchain.chaincode.util.Base64EncodingOps.encode64;
import static nl.medtechchain.chaincode.util.ChaincodeResponseUtil.invalidTransaction;
import static nl.medtechchain.chaincode.util.ChaincodeResponseUtil.successResponse;

@Contract(name = "config", info = @Info(title = "Platform Config Contract", license = @License(name = "Apache 2.0 License", url = "http://www.apache.org/licenses/LICENSE-2.0.html")))
public final class ConfigContract implements ContractInterface {

    private static final Logger logger = Logger.getLogger(ConfigContract.class);

    private static final String CURRENT_NETWORK_CONFIG_KEY = "CURRENT_NETWORK_CONFIG";
    private static final String CURRENT_PLATFORM_CONFIG_KEY = "CURRENT_PLATFORM_CONFIG";

    // Numeric IDs for BFV config entries (proto enum values)
    private static final int BFV_CTX_ID  = 17;
    private static final int BFV_SUM_ID  = 18;
    private static final int BFV_MULT_ID = 19;

    public static NetworkConfig currentNetworkConfig(Context ctx) throws InvalidProtocolBufferException {
        return decode64(ctx.getStub().getStringState(CURRENT_NETWORK_CONFIG_KEY), NetworkConfig::parseFrom);
    }

    public static PlatformConfig currentPlatformConfig(Context ctx) throws InvalidProtocolBufferException {
        return decode64(ctx.getStub().getStringState(CURRENT_PLATFORM_CONFIG_KEY), PlatformConfig::parseFrom);
    }

    @Transaction(intent = Transaction.TYPE.SUBMIT)
    public void Init(Context ctx) {
        var initNetworkConfig = NetworkDefaults.defaultNetworkConfig();
        storeNetworkConfig(ctx, initNetworkConfig);

        var initPlatformConfig = ConfigOps.PlatformConfigOps.create(PlatformConfigDefaults.defaultPlatformConfigs());
        storePlatformConfig(ctx, initPlatformConfig);
    }

    @Transaction(intent = Transaction.TYPE.SUBMIT)
    public String UpdateNetworkConfig(Context ctx, String transaction) {
        try {
            var update = decode64(transaction, UpdateNetworkConfig::parseFrom);
            var current = currentNetworkConfig(ctx);
            storeNetworkConfig(ctx, ConfigOps.NetworkConfigOps.update(current, update.getName(), update.getMapList()));
            logger.info("Updated network config: " + update);
            return encode64(successResponse(transaction));
        } catch (InvalidProtocolBufferException e) {
            logger.warning("Failed to parse NetworkConfig: " + e.getMessage());
            return encode64(invalidTransaction("Failed to parse NetworkConfig: " + e.getMessage()));
        }
    }

    @Transaction(intent = Transaction.TYPE.SUBMIT)
    public String UpdatePlatformConfig(Context ctx, String transaction) {
        try {
            var update = decode64(transaction, UpdatePlatformConfig::parseFrom);

            // Check for BFV switch and presence of required fields
            boolean switchingToBfv = update.getMapList().stream()
                    .anyMatch(e -> e.getKey() == PlatformConfig.Config.CONFIG_FEATURE_QUERY_ENCRYPTION_SCHEME
                            && "bfv".equalsIgnoreCase(e.getValue()));

            if (switchingToBfv) {
                boolean hasCtx  = update.getMapList().stream()
                        .anyMatch(e -> e.getKeyValue() == BFV_CTX_ID);
                boolean hasMult = update.getMapList().stream()
                        .anyMatch(e -> e.getKeyValue() == BFV_MULT_ID);

                if (!(hasCtx && hasMult)) {
                    return encode64(invalidTransaction("Switching encryption to bfv requires cryptocontext and multiply evaluation key."));
                }

                // write files locally for bfv_calc
                try {
                    java.nio.file.Path dir = java.nio.file.Paths.get("fhe_data");
                    java.nio.file.Files.createDirectories(dir);
                    for (PlatformConfig.Entry e : update.getMapList()) {
                        switch (e.getKeyValue()) {
                            case BFV_CTX_ID:
                                java.nio.file.Files.write(dir.resolve("cryptocontext.txt"), java.util.Base64.getDecoder().decode(e.getValue()));
                                break;
                            case BFV_SUM_ID:
                                // sum key currently unused since we don't use it for anything but in the future it will probably be used
                                break;
                            case BFV_MULT_ID:
                                java.nio.file.Files.write(dir.resolve("key-eval-mult.txt"), java.util.Base64.getDecoder().decode(e.getValue()));
                                break;
                            default:
                                break;
                        }
                    }
                } catch (Exception io) {
                    logger.warning("Failed to write BFV keys: " + io.getMessage());
                    return encode64(invalidTransaction("Unable to store BFV evaluation keys on peer", io.toString()));
                }
            }

            var current = currentPlatformConfig(ctx);
            storePlatformConfig(ctx, ConfigOps.PlatformConfigOps.update(current, update.getMapList()));
            logger.info("Updated platform config: " + update);
            return encode64(successResponse(transaction));
        } catch (InvalidProtocolBufferException e) {
            logger.warning("Failed to parse PlatformConfig: " + e.getMessage());
            return encode64(invalidTransaction("Failed to parse PlatformConfig: " + e.getMessage()));
        }
    }

    @Transaction(intent = Transaction.TYPE.EVALUATE)
    public String GetNetworkConfig(Context ctx) {
        try {
            return encode64(successResponse(encode64(currentNetworkConfig(ctx))));
        } catch (InvalidProtocolBufferException e) {
            logger.warning("Failed to parse NetworkConfig: " + e.getMessage());
            return encode64(invalidTransaction("Failed to parse NetworkConfig: " + e.getMessage()));
        }
    }

    @Transaction(intent = Transaction.TYPE.EVALUATE)
    public String GetNetworkConfigId(Context ctx, String id) {
        try {
            var networkConfig = decode64(ctx.getStub().getStringState(TXType.NETWORK_CONFIG.compositeKey(id).toString()), NetworkConfig::parseFrom);
            return encode64(successResponse(encode64(networkConfig)));
        } catch (InvalidProtocolBufferException e) {
            logger.warning("Failed to parse NetworkConfig: " + e.getMessage());
            return encode64(invalidTransaction("Failed to parse NetworkConfig: " + e.getMessage()));
        }
    }

    @Transaction(intent = Transaction.TYPE.EVALUATE)
    public String GetPlatformConfig(Context ctx) {
        try {
            var platformConfig = currentPlatformConfig(ctx);
            return encode64(successResponse(encode64(platformConfig)));
        } catch (InvalidProtocolBufferException e) {
            logger.warning("Failed to parse PlatformConfig: " + e.getMessage());
            return encode64(invalidTransaction("Failed to parse PlatformConfig: " + e.getMessage()));
        }
    }

    @Transaction(intent = Transaction.TYPE.EVALUATE)
    public String GetPlatformConfigId(Context ctx, String id) {
        try {
            var platformConfig = decode64(ctx.getStub().getStringState(TXType.PLATFORM_CONFIG.compositeKey(id).toString()), PlatformConfig::parseFrom);
            return encode64(successResponse(encode64(platformConfig)));
        } catch (InvalidProtocolBufferException e) {
            logger.warning("Failed to parse PlatformConfig: " + e.getMessage());
            return encode64(invalidTransaction("Failed to parse PlatformConfig: " + e.getMessage()));
        }
    }

    private void storeNetworkConfig(Context ctx, NetworkConfig networkConfig) {
        ctx.getStub().putStringState(CURRENT_NETWORK_CONFIG_KEY, encode64(networkConfig));
        ctx.getStub().putStringState(TXType.NETWORK_CONFIG.compositeKey(networkConfig.getId()).toString(), encode64(networkConfig));
    }

    private void storePlatformConfig(Context ctx, PlatformConfig platformConfig) {
        ctx.getStub().putStringState(CURRENT_PLATFORM_CONFIG_KEY, encode64(platformConfig));
        ctx.getStub().putStringState(TXType.PLATFORM_CONFIG.compositeKey(platformConfig.getId()).toString(), encode64(platformConfig));
    }

}