package nl.medtechchain.chaincode.service.query;

import com.google.protobuf.Timestamp;
import nl.medtechchain.proto.devicedata.DeviceCategory;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.devicedata.MedicalSpeciality;

import java.util.*;

// Generates test assets with specific field values and encryption states
public class TestDataGenerator {
    
    private Random random;
    private Timestamp base = Timestamp.newBuilder().setSeconds(1748189319).build();
    
    public TestDataGenerator() {
        this.random = new Random();
    }
    
    public TestDataGenerator(long seed) {
        this.random = new Random(seed);
    }
    
    // represents "encrypted" values in tests  
    public static class Ciphertext {
        private final String ciphertext;
        private final String version;
        
        public Ciphertext(String ciphertext, String version) {
            this.ciphertext = ciphertext;
            this.version = version;
        }
        
        public String getCiphertext() {
            return ciphertext;
        }
        
        public String getVersion() {
            return version;
        }
    }
    
    public List<DeviceDataAsset> generateAssetsWithCounts(
            Map<String, Map<Object, Integer>> fieldSpecs, 
            int totalAssets) {
        
        List<DeviceDataAsset> assets = new ArrayList<>();
        Set<Object> usedValues = new HashSet<>();
        
        // Generate assets with specified field values
        for (String fieldName : fieldSpecs.keySet()) {
            Map<Object, Integer> valueCounts = fieldSpecs.get(fieldName);
            for (Map.Entry<Object, Integer> entry : valueCounts.entrySet()) {
                Object value = entry.getKey();
                int count = entry.getValue();
                usedValues.add(value);
                
                for (int i = 0; i < count; i++) {
                    Map<String, Object> fieldValues = new HashMap<>();
                    fieldValues.put(fieldName, value);
                    
                    // figure out the version for this asset
                    String version = "plaintext";
                    if (value instanceof Ciphertext) {
                        version = ((Ciphertext) value).getVersion();
                    }
                    
                    assets.add(generateAsset(fieldValues, version));
                }
            }
        }
        
        // fill remaining with random data
        while (assets.size() < totalAssets) {
            Map<String, Object> fieldValues = new HashMap<>();
            for (String fieldName : fieldSpecs.keySet()) {
                fieldValues.put(fieldName, generateRandomValueExcept(fieldName, usedValues));
            }
            assets.add(generateAsset(fieldValues, "plaintext"));
        }
        
        Collections.shuffle(assets, random);
        return assets;
    }
    
    // New method that properly coordinates multiple fields in same assets
    public List<DeviceDataAsset> generateAssetsWithCoordinatedFields(
            Map<String, Object> commonFieldValues,
            int totalAssets) {
        
        List<DeviceDataAsset> assets = new ArrayList<>();
        
        // Generate all assets with the same field values
        for (int i = 0; i < totalAssets; i++) {
            Map<String, Object> fieldValues = new HashMap<>(commonFieldValues);
            
            // figure out the version for this asset
            String version = "plaintext";
            for (Object value : commonFieldValues.values()) {
                if (value instanceof Ciphertext) {
                    version = ((Ciphertext) value).getVersion();
                    break;
                }
            }
            
            assets.add(generateAsset(fieldValues, version));
        }
        
        Collections.shuffle(assets, random);
        return assets;
    }
    
    public DeviceDataAsset generateAsset(Map<String, Object> fieldValues) {
        return generateAsset(fieldValues, "plaintext");
    }
    
    public DeviceDataAsset generateAsset(Map<String, Object> fieldValues, String keyVersion) {
        DeviceDataAsset.Builder asset = DeviceDataAsset.newBuilder()
                .setTimestamp(base)
                .setConfigId(randomString(10))
                .setKeyVersion(keyVersion);
        
        DeviceDataAsset.DeviceData.Builder data = asset.getDeviceDataBuilder();
        
        // set all the field values 
        data.setHospital(stringField(fieldValues.getOrDefault("hospital", randomHospital())));
        data.setManufacturer(stringField(fieldValues.getOrDefault("manufacturer", randomManufacturer())));
        data.setModel(stringField(fieldValues.getOrDefault("model", randomModel())));
        data.setFirmwareVersion(stringField(fieldValues.getOrDefault("firmware_version", randomFirmwareVersion())));
        data.setDeviceType(stringField(fieldValues.getOrDefault("device_type", randomDeviceType())));
        
        data.setUsageHours(intField(fieldValues.getOrDefault("usage_hours", randomUsageHours())));
        data.setBatteryLevel(intField(fieldValues.getOrDefault("battery_level", randomBatteryLevel())));
        data.setSyncFrequencySeconds(intField(fieldValues.getOrDefault("sync_frequency_seconds", randomSyncFrequency())));
        
        data.setProductionDate(timestampField(fieldValues.getOrDefault("production_date", randomProductionDate())));
        data.setLastServiceDate(timestampField(fieldValues.getOrDefault("last_service_date", randomLastServiceDate())));
        data.setWarrantyExpiryDate(timestampField(fieldValues.getOrDefault("warranty_expiry_date", randomWarrantyExpiryDate())));
        data.setLastSyncTime(timestampField(fieldValues.getOrDefault("last_sync_time", randomLastSyncTime())));
        
        data.setActiveStatus(boolField(fieldValues.getOrDefault("active_status", randomActiveStatus())));
        data.setSpeciality(medicalSpecialityField(fieldValues.getOrDefault("speciality", randomMedicalSpeciality())));
        data.setCategory(deviceCategoryField(fieldValues.getOrDefault("category", randomDeviceCategory())));
        
        return asset.build();
    }
    
    // field builders - handle both plain and encrypted data
    private DeviceDataAsset.StringField stringField(Object value) {
        if (value instanceof Ciphertext) {
            Ciphertext ct = (Ciphertext) value;
            return DeviceDataAsset.StringField.newBuilder().setEncrypted(ct.getCiphertext()).build();
        } else if (value instanceof String) {
            return DeviceDataAsset.StringField.newBuilder().setPlain((String) value).build();
        } else {
            throw new IllegalArgumentException("Invalid type for string field: " + value.getClass());
        }
    }
    
    private DeviceDataAsset.IntegerField intField(Object value) {
        if (value instanceof Ciphertext) {
            Ciphertext ct = (Ciphertext) value;
            return DeviceDataAsset.IntegerField.newBuilder().setEncrypted(ct.getCiphertext()).build();
        } else if (value instanceof Integer) {
            return DeviceDataAsset.IntegerField.newBuilder().setPlain((Integer) value).build();
        } else {
            throw new IllegalArgumentException("Invalid type for integer field: " + value.getClass());
        }
    }
    
    private DeviceDataAsset.TimestampField timestampField(Object value) {
        if (value instanceof Ciphertext) {
            Ciphertext ct = (Ciphertext) value;
            return DeviceDataAsset.TimestampField.newBuilder().setEncrypted(ct.getCiphertext()).build();
        } else if (value instanceof Timestamp) {
            return DeviceDataAsset.TimestampField.newBuilder().setPlain((Timestamp) value).build();
        } else {
            throw new IllegalArgumentException("Invalid type for timestamp field: " + value.getClass());
        }
    }
    
    private DeviceDataAsset.BoolField boolField(Object value) {
        if (value instanceof Ciphertext) {
            Ciphertext ct = (Ciphertext) value;
            return DeviceDataAsset.BoolField.newBuilder().setEncrypted(ct.getCiphertext()).build();
        } else if (value instanceof Boolean) {
            return DeviceDataAsset.BoolField.newBuilder().setPlain((Boolean) value).build();
        } else {
            throw new IllegalArgumentException("Invalid type for boolean field: " + value.getClass());
        }
    }
    
    private DeviceDataAsset.MedicalSpecialityField medicalSpecialityField(Object value) {
        if (value instanceof Ciphertext) {
            Ciphertext ct = (Ciphertext) value;
            return DeviceDataAsset.MedicalSpecialityField.newBuilder().setEncrypted(ct.getCiphertext()).build();
        } else if (value instanceof MedicalSpeciality) {
            return DeviceDataAsset.MedicalSpecialityField.newBuilder().setPlain((MedicalSpeciality) value).build();
        } else {
            throw new IllegalArgumentException("Invalid type for medical speciality field: " + value.getClass());
        }
    }
    
    private DeviceDataAsset.DeviceCategoryField deviceCategoryField(Object value) {
        if (value instanceof Ciphertext) {
            Ciphertext ct = (Ciphertext) value;
            return DeviceDataAsset.DeviceCategoryField.newBuilder().setEncrypted(ct.getCiphertext()).build();
        } else if (value instanceof DeviceCategory) {
            return DeviceDataAsset.DeviceCategoryField.newBuilder().setPlain((DeviceCategory) value).build();
        } else {
            throw new IllegalArgumentException("Invalid type for device category field: " + value.getClass());
        }
    }
    
    // Random value generators
    private Object generateRandomValueExcept(String fieldName, Set<Object> excludeValues) {
        Object value;
        do {
            value = generateRandomValue(fieldName);
        } while (excludeValues.contains(value));
        return value;
    }
    
    private Object generateRandomValue(String fieldName) {
        switch (fieldName) {
            case "hospital": return randomHospital();
            case "manufacturer": return randomManufacturer();
            case "model": return randomModel();
            case "firmware_version": return randomFirmwareVersion();
            case "device_type": return randomDeviceType();
            case "usage_hours": return randomUsageHours();
            case "battery_level": return randomBatteryLevel();
            case "sync_frequency_seconds": return randomSyncFrequency();
            case "production_date": return randomProductionDate();
            case "last_service_date": return randomLastServiceDate();
            case "warranty_expiry_date": return randomWarrantyExpiryDate();
            case "last_sync_time": return randomLastSyncTime();
            case "active_status": return randomActiveStatus();
            case "speciality": return randomMedicalSpeciality();
            case "category": return randomDeviceCategory();
            default: return null;
        }
    }
    
    // Random generators
    private String randomString(int length) {
        String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < length; i++) {
            result.append(alphabet.charAt(random.nextInt(alphabet.length())));
        }
        return result.toString();
    }
    
    private String randomHospital() {
        return "Hospital_" + randomString(5);
    }
    
    private String randomManufacturer() {
        return "Manufacturer_" + randomString(4);
    }
    
    private String randomModel() {
        return "Model_" + randomString(3);
    }
    
    private String randomFirmwareVersion() {
        return "v" + random.nextInt(10) + "." + random.nextInt(10);
    }
    
    private String randomDeviceType() {
        return "Type_" + randomString(4);
    }
    
    private Integer randomUsageHours() {
        return random.nextInt(10000);
    }
    
    private Integer randomBatteryLevel() {
        return random.nextInt(101);
    }
    
    private Integer randomSyncFrequency() {
        return random.nextInt(3600);
    }
    
    private Timestamp randomProductionDate() {
        return Timestamp.newBuilder()
                .setSeconds(base.getSeconds() - random.nextInt(365 * 24 * 3600))
                .build();
    }
    
    private Timestamp randomLastServiceDate() {
        return Timestamp.newBuilder()
                .setSeconds(base.getSeconds() - random.nextInt(90 * 24 * 3600))
                .build();
    }
    
    private Timestamp randomWarrantyExpiryDate() {
        return Timestamp.newBuilder()
                .setSeconds(base.getSeconds() + random.nextInt(365 * 24 * 3600))
                .build();
    }
    
    private Timestamp randomLastSyncTime() {
        return Timestamp.newBuilder()
                .setSeconds(base.getSeconds() - random.nextInt(30 * 24 * 3600))
                .build();
    }
    
    private Boolean randomActiveStatus() {
        return random.nextBoolean();
    }
    
    private MedicalSpeciality randomMedicalSpeciality() {
        MedicalSpeciality[] values = MedicalSpeciality.values();
        MedicalSpeciality speciality;
        do {
            speciality = values[random.nextInt(values.length)];
        } while (speciality == MedicalSpeciality.UNRECOGNIZED || 
                 speciality == MedicalSpeciality.MEDICAL_SPECIALITY_UNSPECIFIED);
        return speciality;
    }
    
    private DeviceCategory randomDeviceCategory() {
        DeviceCategory[] values = DeviceCategory.values();
        DeviceCategory category;
        do {
            category = values[random.nextInt(values.length)];
        } while (category == DeviceCategory.UNRECOGNIZED || 
                 category == DeviceCategory.DEVICE_CATEGORY_UNSPECIFIED);
        return category;
    }
} 