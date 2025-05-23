package nl.medtechchain.chaincode.contract;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import nl.medtechchain.chaincode.service.encryption.PlatformEncryptionInterface;
import nl.medtechchain.chaincode.service.query.FilterService;
import nl.medtechchain.chaincode.service.query.QueryService;
import nl.medtechchain.proto.config.PlatformConfig;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.query.*;
import org.hyperledger.fabric.Logger;
import org.hyperledger.fabric.contract.Context;
import org.hyperledger.fabric.contract.ContractInterface;
import org.hyperledger.fabric.contract.annotation.Contract;
import org.hyperledger.fabric.contract.annotation.Info;
import org.hyperledger.fabric.contract.annotation.License;
import org.hyperledger.fabric.contract.annotation.Transaction;
import org.hyperledger.fabric.shim.ledger.KeyValue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;

import static nl.medtechchain.chaincode.util.Base64EncodingOps.decode64;
import static nl.medtechchain.chaincode.util.Base64EncodingOps.encode64;
import static nl.medtechchain.chaincode.util.ChaincodeResponseUtil.*;
import static nl.medtechchain.chaincode.util.MeasureExecTimeUtil.monitorTime;

@Contract(name = "devicedata", info = @Info(title = "Device Data Contract", license = @License(name = "Apache 2.0 License", url = "http://www.apache.org/licenses/LICENSE-2.0.html")))
public final class DeviceDataContract implements ContractInterface {

    private static final Logger logger = Logger.getLogger(DeviceDataContract.class);

    @Transaction(intent = Transaction.TYPE.SUBMIT)
    public String StoreDeviceData(Context ctx, String id, String transaction) {
        try {
            decode64(transaction, DeviceDataAsset::parseFrom);
            var key = TXType.DEVICE_DATA_ASSET.compositeKey(id);
            ctx.getStub().putStringState(key.toString(), transaction);
            logger.debug("Stored device data asset: " + key);
            return encode64(successResponse(transaction));
        } catch (InvalidProtocolBufferException e) {
            logger.warning("Failed to parse DeviceDataAsset: " + e.getMessage());
            logger.log(Level.WARNING, "Failed to parse DeviceDataAsset", e);
            return encode64(invalidTransaction("Failed to parse DeviceDataAsset: " + e.getMessage()));
        }
    }

    @Transaction(intent = Transaction.TYPE.SUBMIT)
    public String Query(Context ctx, String transaction) {
        try {
            var startTime = Instant.now().getEpochSecond();
            var platformConfig = ConfigContract.currentPlatformConfig(ctx);
            var query = decode64(transaction, Query::parseFrom);
            var queryService = new QueryService(platformConfig);

            var error = queryService.validateQuery(query);
            if (error.isPresent())
                return encode64(errorResponse(error.get()));

            logger.info("Performing: " + query);

            var result = monitorTime(() -> {
                var data = getFilteredData(ctx, query, platformConfig);
                var r = QueryResult.newBuilder().setError(invalidTransaction("Unknown query type").getError()).build();
                try {
                    switch (query.getQueryType()) {
                        case COUNT:
                            r = queryService.count(query, data);
                            break;
                        case GROUPED_COUNT:
                            r = queryService.groupedCount(query, data);
                            break;
                        case AVERAGE:
                            r = queryService.average(query, data);
                            break;
                        case SUM:
                            r = queryService.sum(query, data);
                            break;
                        case UNIQUE_COUNT:
                            r = queryService.uniqueCount(query, data);
                            break;
                        case HISTOGRAM:
                            r = queryService.histogram(query, data);
                            break;
                        case STD:
                            r = queryService.std(query, data);
                            break;
                    }
                } catch (Exception t) {
                    logger.log(Level.WARNING, "Query error", t);
                    return QueryResult.newBuilder().setError(internalError("Error running query", t.toString()).getError()).build();
                }

                return r;
            });

            var endTime = Instant.now().getEpochSecond();

            var asset = QueryAsset.newBuilder().setQuery(query).setResult(result).setRequestTime(Timestamp.newBuilder().setSeconds(startTime).build()).setResponseTime(Timestamp.newBuilder().setSeconds(endTime).build()).build();
            var key = TXType.QUERY.compositeKey(UUID.nameUUIDFromBytes(asset.toByteArray()).toString());
            ctx.getStub().putStringState(key.toString(), encode64(asset));

            if (result.hasError())
                return encode64(errorResponse(result.getError()));

            return encode64(successResponse(encode64(result)));
        } catch (InvalidProtocolBufferException e) {
            logger.log(Level.WARNING, "Failed to parse query transaction", e);
            return encode64(invalidTransaction("Error parsing query transaction", e.toString()));
        }
    }

    @Transaction(intent = Transaction.TYPE.EVALUATE)
    public String ReadQueries(Context ctx, String transaction) {

        try {
            var readPage = decode64(transaction, ReadQueryAssetPage::parseFrom);

            var skip = (readPage.getPageNumber() - 1) * readPage.getPageSize();

            var result = new ArrayList<QueryAsset>();
            var iterator = ctx.getStub().getStateByPartialCompositeKey(TXType.QUERY.partialKey());


            for (KeyValue kv : iterator) {
                if (skip > 0) {
                    skip--;
                    continue;
                }

                try {
                    result.add(decode64(kv.getStringValue(), QueryAsset::parseFrom));
                } catch (InvalidProtocolBufferException e) {
                    logger.warning("Error parsing device data transaction from ledger: " + e.getMessage() + "\n" + kv.getKey() + "\n" + Arrays.toString(kv.getValue()));
                    break;
                }

                if (result.size() == readPage.getPageSize())
                    break;
            }

            return encode64(QueryAssetPage.newBuilder().setPageSize(readPage.getPageSize()).setPageNumber(readPage.getPageNumber()).addAllAssets(result).build());
        } catch (InvalidProtocolBufferException e) {
            logger.log(Level.WARNING, "Failed to parse ReadQueryAssetPage", e);
            return encode64(invalidTransaction("Failed to parse ReadQueryAssetPage: " + e.getMessage()));
        }
    }

    private List<DeviceDataAsset> getFilteredData(Context ctx, Query tx, PlatformConfig platformConfig) {
        var filteredDeviceData = new ArrayList<DeviceDataAsset>();

        var iterator = ctx.getStub().getStateByPartialCompositeKey(TXType.DEVICE_DATA_ASSET.partialKey());

        var filterService = PlatformEncryptionInterface.Factory.getInstance(platformConfig).map(FilterService::new).orElse(new FilterService());

        for (KeyValue kv : iterator) {
            try {
                DeviceDataAsset asset = decode64(kv.getStringValue(), DeviceDataAsset::parseFrom);

                if (asset.getTimestamp().getSeconds() < tx.getStartTime().getSeconds() || asset.getTimestamp().getSeconds() > tx.getEndTime().getSeconds())
                    continue;

                boolean valid = asset.getConfigId().equals(platformConfig.getId()) &&
                        tx.getFiltersList().stream().allMatch(filter -> filterService.checkFilter(asset, filter));

                if (valid)
                    filteredDeviceData.add(asset);

            } catch (InvalidProtocolBufferException e) {
                logger.log(Level.WARNING, "Error parsing device data transaction from ledger", e);
                logger.warning("Error parsing device data transaction from ledger: " + e.getMessage() + "\n" + kv.getKey() + "\n" + Arrays.toString(kv.getValue()));
            }
        }

        return filteredDeviceData;
    }
}