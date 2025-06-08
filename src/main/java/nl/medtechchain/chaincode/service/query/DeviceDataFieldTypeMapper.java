package nl.medtechchain.chaincode.service.query;

import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.devicedata.DeviceDataFieldType;

import java.util.Optional;

import static nl.medtechchain.proto.devicedata.DeviceDataFieldType.*;

// Maps field names to types for protobuf fields
public class DeviceDataFieldTypeMapper {

    public static DeviceDataFieldType fromFieldName(String name) {
        // all fields are wrapped in a protobuf message
        var descriptor = Optional.ofNullable(DeviceDataAsset.DeviceData.getDescriptor().findFieldByName(name));
        if (descriptor.isEmpty())
            return UNRECOGNIZED;

        switch (descriptor.get().getMessageType().getFullName()) {
            case "devicedata.DeviceDataAsset.StringField":
                return STRING;
            case "devicedata.DeviceDataAsset.TimestampField":
                return TIMESTAMP;
            case "devicedata.DeviceDataAsset.IntegerField":
                return INTEGER;
            case "devicedata.DeviceDataAsset.BoolField":
                return BOOL;
            case "devicedata.DeviceDataAsset.MedicalSpecialityField":
                return MEDICAL_SPECIALITY;
            case "devicedata.DeviceDataAsset.DeviceCategoryField":
                return DEVICE_CATEGORY;
        }

        return DEVICE_DATA_FIELD_TYPE_UNSPECIFIED;
    }
} 