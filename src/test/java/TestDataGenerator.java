package nl.medtechchain.chaincode.test;

import com.google.protobuf.Timestamp;
import nl.medtechchain.proto.devicedata.DeviceCategory;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.devicedata.MedicalSpeciality;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class TestDataGenerator {
    /*
     * StringField hospital = 1;
     * StringField manufacturer = 2;
     * StringField model = 3;
     * StringField firmware_version = 4;
     * StringField device_type = 5;
     * TimestampField production_date = 6;
     * TimestampField last_service_date = 7;
     * TimestampField warranty_expiry_date = 8;
     * TimestampField last_sync_time = 9;
     * IntegerField usage_hours = 10;
     * IntegerField battery_level = 11;
     * IntegerField sync_frequency_seconds = 12;
     * BoolField active_status = 13;
     * MedicalSpecialityField speciality = 14;
     * DeviceCategoryField category = 15;
     */

    long seed;
    Random random;

    // used as base for all generations as a single definition of "now"
    Timestamp base = Timestamp.newBuilder().setSeconds(1748189319).build();

    // no seed
    public TestDataGenerator() {
        this.random = new Random();
    }

    // with seed
    public TestDataGenerator(long seed) {
        this.seed = seed;
        this.random = new Random(seed);
    }

    public List<DeviceDataAsset> generateDeviceDataAssets(Map<String, Object> map, int n) {
        List<DeviceDataAsset> assets = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            assets.add(generateDeviceDataAsset(map));
        }
        return assets;
    }

    public DeviceDataAsset generateDeviceDataAsset(Map<String, Object> map) {
        // default stuff on each asset
        DeviceDataAsset.Builder asset = DeviceDataAsset.newBuilder().setTimestamp(randomTimestamp(base, 31))
                .setConfigId(UUID.randomUUID().toString()).build();

        DeviceDataAsset.DeviceData.Builder data = asset.getDataBuilder();

        // stings
        data.setHospital(stringField(map.getOrDefault("hospital", randomHospital())));
        data.setManufacturer(stringField(map.getOrDefault("manufacturer", randomManufacturer())));
        data.setModel(stringField(map.getOrDefault("model", randomModel())));
        data.setFirmwareVersion(stringField(map.getOrDefault("firmware_version", randomFirmwareVersion())));
        data.setDeviceType(stringField(map.getOrDefault("device_type", randomDeviceType())));

        // ints
        data.setUsageHours(intField(map.getOrDefault("usage_hours", randomUsageHours())));
        data.setBatteryLevel(intField(map.getOrDefault("battery_level", randomBatteryLevel())));
        data.setSyncFrequencySeconds(intField(map.getOrDefault("sync_frequency_seconds", randomSyncFrequencySeconds())));

        // timestamps
        data.setProductionDate(timestampField(map.getOrDefault("production_date", randomProductionDate())));
        data.setLastServiceDate(timestampField(map.getOrDefault("last_service_date", randomLastServiceDate())));
        data.setWarrantyExpiryDate(timestampField(map.getOrDefault("warranty_expiry_date", randomWarrantyExpiryDate())));
        data.setLastSyncTime(timestampField(map.getOrDefault("last_sync_time", randomLastSyncTime())));

        // bools
        data.setActiveStatus(boolField(map.getOrDefault("active_status", randomActiveStatus())));

        // enums    
        data.setSpeciality(medField(map.getOrDefault("speciality", randomMedicalSpeciality())));
        data.setCategory(catField(map.getOrDefault("category", randomDeviceCategory())));

        return asset.build();
    }

      ////////////          /           /////////
     /// From here on is just boring helpers ///
    /////////            /         ////////////

    // translations to devicedataasset fields
    private static DeviceDataAsset.StringField stringField(String v) {
        return DeviceDataAsset.StringField.newBuilder().setPlain(v).build();
    }

    private static DeviceDataAsset.IntegerField intField(int v) {
        return DeviceDataAsset.IntegerField.newBuilder().setPlain(v).build();
    }

    private static DeviceDataAsset.TimestampField timestampField(Timestamp v) {
        return DeviceDataAsset.TimestampField.newBuilder().setPlain(v).build();
    }

    private static DeviceDataAsset.BoolField boolField(boolean b) {
        return DeviceDataAsset.BoolField.newBuilder().setPlain(b).build();
    }

    private static DeviceDataAsset.MedicalSpecialityField medField(MedicalSpeciality m) {
        return DeviceDataAsset.MedicalSpecialityField.newBuilder().setPlain(m).build();
    }

    private static DeviceDataAsset.DeviceCategoryField catField(DeviceCategory c) {
        return DeviceDataAsset.DeviceCategoryField.newBuilder().setPlain(c).build();
    }
    // random generators
    // reuses the Random instance
    private int randomInt(int min, int max) {
        return random.nextInt(max - min + 1) + min;
    }

    private int randomBool() {
        return random.nextBoolean();
    }

    private String randomString(int len) {
        String alphabet = " ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        String result = "";
        for (int i = 0; i < len; i++) {
            result += alphabet.charAt(random.nextInt(alphabet.length()));
        }
        return result;
    }

    private Timestamp randomPastTimestamp(Timestamp now, int pastDaysBound) {
        return Timestamp.newBuilder().setSeconds(now.getSeconds() - random.nextInt(3600 * 24 * pastDaysBound)).build();
    }

    private Timestamp randomFutureTimestamp(Timestamp now, int futureDaysBound) {
        return Timestamp.newBuilder().setSeconds(now.getSeconds() + random.nextInt(3600 * 24 * futureDaysBound))
                .build();
    }

    private String randomHospital() {
        return "Hospital " + randomString(10);
    }

    private String randomManufacturer() {
        return "Manufacturer " + randomString(6);
    }

    private String randomModel() {
        return "Model " + randomString(3);
    }

    private String randomFirmwareVersion() {
        return "Firmware " + randomString(4);
    }

    private String randomDeviceType() {
        return "Type " + randomString(5);
    }

    // values is default java method for enums
    private MedicalSpeciality randomMedicalSpeciality() {
        return MedicalSpeciality.values()[random.nextInt(MedicalSpeciality.values().length)];
    }

    private DeviceCategory randomDeviceCategory() {
        return DeviceCategory.values()[random.nextInt(DeviceCategory.values().length)];
    }

    private boolean randomActiveStatus() {
        return random.nextBoolean();
    }

    private Timestamp randomLastSyncTime() {
        return randomPastTimestamp(base, 31);
    }

    private int randomUsageHours() {
        return random.nextInt(10000);
    }

    private Timestamp randomLastServiceDate() {
        return randomPastTimestamp(base, 31);
    }

    private Timestamp randomWarrantyExpiryDate() {
        return randomFutureTimestamp(base, 31);
    }

    private Timestamp randomProductionDate() {
        return randomPastTimestamp(base, 31);
    }

    private int randomBatteryLevel() {
        return random.nextInt(100);
    }

    private int randomSyncFrequencySeconds() {
        return random.nextInt(3600);
    }
}
