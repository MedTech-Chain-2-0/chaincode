package nl.medtechchain.chaincode.service.query;

import com.google.protobuf.Timestamp;
import nl.medtechchain.chaincode.service.encryption.EncryptionService;
import nl.medtechchain.proto.devicedata.DeviceCategory;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.devicedata.MedicalSpeciality;
import nl.medtechchain.proto.query.Filter;

import java.util.Optional;
import java.util.logging.Logger;

// Handles filtering device data - works with both encrypted and plain fields
public class FilterService {

    private static final Logger logger = Logger.getLogger(FilterService.class.getName());

    private final EncryptionService encryptionService;

    public FilterService() {
        this.encryptionService = null;
    }

    public FilterService(EncryptionService encryptionService) {
        this.encryptionService = encryptionService;
    }

    public boolean checkFilter(DeviceDataAsset asset, Filter filter) {
        try {
            var descriptor = Optional.ofNullable(DeviceDataAsset.DeviceData.getDescriptor().findFieldByName(filter.getField()));
            if (descriptor.isEmpty())
                throw new IllegalStateException("Field " + filter.getField() + " is not present in asset " + asset);

            var value = asset.getDeviceData().getField(descriptor.get());
            var keyVersion = asset.getKeyVersion();


            switch (DeviceDataFieldTypeMapper.fromFieldName(filter.getField())) {
                case STRING:
                    assert filter.getComparatorCase() == Filter.ComparatorCase.STRING_FILTER;
                    return check(filter.getField(), (DeviceDataAsset.StringField) value, filter.getStringFilter(), keyVersion);
                case INTEGER:
                    assert filter.getComparatorCase() == Filter.ComparatorCase.INTEGER_FILTER;
                    return check(filter.getField(), (DeviceDataAsset.IntegerField) value, filter.getIntegerFilter(), keyVersion);
                case TIMESTAMP:
                    assert filter.getComparatorCase() == Filter.ComparatorCase.TIMESTAMP_FILTER;
                    return check(filter.getField(), (DeviceDataAsset.TimestampField) value, filter.getTimestampFilter(), keyVersion);
                case BOOL:
                    assert filter.getComparatorCase() == Filter.ComparatorCase.BOOL_FILTER;
                    return check(filter.getField(), (DeviceDataAsset.BoolField) value, filter.getBoolFilter(), keyVersion);
                case DEVICE_CATEGORY:
                    assert filter.getComparatorCase() == Filter.ComparatorCase.ENUM_FILTER;
                    return check(filter.getField(), (DeviceDataAsset.DeviceCategoryField) value, filter.getEnumFilter(), keyVersion);
                case MEDICAL_SPECIALITY:
                    assert filter.getComparatorCase() == Filter.ComparatorCase.ENUM_FILTER;
                    return check(filter.getField(), (DeviceDataAsset.MedicalSpecialityField) value, filter.getEnumFilter(), keyVersion);
            }

            return false;
        } catch (Throwable t) {
            logger.warning("Error checking filter: " + filter + ". " + t);
            return false;
        }
    }

    private boolean check(String name, DeviceDataAsset.StringField field, Filter.StringFilter filter, String keyVersion) {
        String value;
        switch (field.getFieldCase()) {
            case PLAIN:
                value = field.getPlain();
                break;
            case ENCRYPTED:
                if (encryptionService == null)
                    throw new IllegalStateException("Field " + name + " is encrypted, but the platform is not properly configured to use encryption.");
                // uses the key version from the asset
                value = encryptionService.decryptString(field.getEncrypted(), keyVersion);
                break;
            default:
                return false;
        }

        switch (filter.getOperator()) {
            case CONTAINS:
                return value.contains(filter.getValue());
            case ENDS_WITH:
                return value.endsWith(filter.getValue());
            case EQUALS:
                return value.equals(filter.getValue());
            case STARTS_WITH:
                return value.startsWith(filter.getValue());
        }

        return false;
    }

    private boolean check(String name, DeviceDataAsset.IntegerField field, Filter.IntegerFilter filter, String keyVersion) {
        long value;
        switch (field.getFieldCase()) {
            case PLAIN:
                value = field.getPlain();
                break;
            case ENCRYPTED:
                if (encryptionService == null)
                    throw new IllegalStateException("Field " + name + " is encrypted, but the platform is not properly configured to use encryption.");
                // uses the key version from the asset
                value = encryptionService.decryptLong(field.getEncrypted(), keyVersion);
                break;
            default:
                return false;
        }

        switch (filter.getOperator()) {
            case GREATER_THAN_OR_EQUAL:
                return value >= filter.getValue();
            case EQUALS:
                return value == filter.getValue();
            case LESS_THAN:
                return value < filter.getValue();
            case GREATER_THAN:
                return value > filter.getValue();
            case LESS_THAN_OR_EQUAL:
                return value <= filter.getValue();
        }

        return false;
    }

    private boolean check(String name, DeviceDataAsset.TimestampField field, Filter.TimestampFilter filter, String keyVersion) {
        Timestamp value;
        switch (field.getFieldCase()) {
            case PLAIN:
                value = field.getPlain();
                break;
            case ENCRYPTED:
                if (encryptionService == null)
                    throw new IllegalStateException("Field " + name + " is encrypted, but the platform is not properly configured to use encryption.");
                // uses the key version from the asset
                value = Timestamp.newBuilder().setSeconds(encryptionService.decryptLong(field.getEncrypted(), keyVersion)).build();
                break;
            default:
                return false;
        }

        switch (filter.getOperator()) {
            case AFTER:
                return value.getSeconds() > filter.getValue().getSeconds();
            case BEFORE:
                return value.getSeconds() < filter.getValue().getSeconds();
            case EQUALS:
                return value.getSeconds() == filter.getValue().getSeconds();
        }

        return false;
    }

    private boolean check(String name, DeviceDataAsset.BoolField field, Filter.BoolFilter filter, String keyVersion) {
        boolean value;
        switch (field.getFieldCase()) {
            case PLAIN:
                value = field.getPlain();
                break;
            case ENCRYPTED:
                if (encryptionService == null)
                    throw new IllegalStateException("Field " + name + " is encrypted, but the platform is not properly configured to use encryption.");
                // uses the key version from the asset
                value = encryptionService.decryptBool(field.getEncrypted(), keyVersion);
                break;
            default:
                return false;
        }

        if (filter.getOperator() == Filter.BoolFilter.BoolOperator.EQUALS) {
            return value == filter.getValue();
        }

        return false;
    }

    private boolean check(String name, DeviceDataAsset.MedicalSpecialityField field, Filter.EnumFilter filter, String keyVersion) {
        MedicalSpeciality value;
        switch (field.getFieldCase()) {
            case PLAIN:
                value = field.getPlain();
                break;
            case ENCRYPTED:
                if (encryptionService == null)
                    throw new IllegalStateException("Field " + name + " is encrypted, but the platform is not properly configured to use encryption.");
                // uses the key version from the asset
                value = MedicalSpeciality.forNumber((int) encryptionService.decryptLong(field.getEncrypted(), keyVersion));
                break;
            default:
                return false;
        }

        return value == MedicalSpeciality.valueOf(filter.getValue());
    }

    private boolean check(String name, DeviceDataAsset.DeviceCategoryField field, Filter.EnumFilter filter, String keyVersion) {
        DeviceCategory value;
        switch (field.getFieldCase()) {
            case PLAIN:
                value = field.getPlain();
                break;
            case ENCRYPTED:
                if (encryptionService == null)
                    throw new IllegalStateException("Field " + name + " is encrypted, but the platform is not properly configured to use encryption.");
                // uses the key version from the asset
                value = DeviceCategory.forNumber((int) encryptionService.decryptLong(field.getEncrypted(), keyVersion));
                break;
            default:
                return false;
        }

        return value == DeviceCategory.valueOf(filter.getValue());
    }
} 