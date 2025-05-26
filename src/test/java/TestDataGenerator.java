package nl.medtechchain.chaincode.test;

import com.google.protobuf.Timestamp;
import nl.medtechchain.proto.devicedata.DeviceCategory;
import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.devicedata.MedicalSpeciality;

import java.util.*;

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

    public List<DeviceDataAsset> generateDeviceDataAssetsWithCount(Map<String, Map<Object, Integer>> map, int n) {
        List<DeviceDataAsset> assets = new ArrayList<>();
        Set<Object> excepts = new HashSet<>();

        // nothing specified
        if(map.isEmpty()){
            return generateDeviceDataAssets(new HashMap<>(), n);
        }

        // for each string(field) get the object(value for that field) and do it Integer(count) times.
        for(String key : map.keySet()){
            Map<Object, Integer> value = map.get(key);
            for(Map.Entry<Object, Integer> entry : value.entrySet()){
                Object obj = entry.getKey();
                excepts.add(obj);
                int count = entry.getValue();
                for(int i = 0; i < count; i++){
                    Map<String, Object> mapy = new HashMap<>();
                    mapy.put(key, obj);
                    assets.add(generateDeviceDataAsset(mapy));
                }
            }
        }

        while(assets.size() < n){        
            Map<String, Object> mapy = new HashMap<>();
            for(String key : map.keySet()){
                mapy.put(key, randomExcept(key, excepts));
            }
            assets.add(generateDeviceDataAsset(mapy));
        }

        Collections.shuffle(assets); // why not
        return assets;
    }

    /*
     * @param map: String, a name of field, Object value that field should have
     * @param n: int, number of assets to generate
     * @return List<DeviceDataAsset>
     */

    public List<DeviceDataAsset> generateDeviceDataAssets(Map<String, Object> map, int n) {
        List<DeviceDataAsset> assets = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            assets.add(generateDeviceDataAsset(map));
        }
        return assets;
    }

    public DeviceDataAsset generateDeviceDataAsset(Map<String, Object> map) {
        // default stuff on each asset
        DeviceDataAsset.Builder asset = DeviceDataAsset.newBuilder().setTimestamp(base)
                .setConfigId(UUID.randomUUID().toString());

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

    private static Object randomExcept(String field, Set<Object> excepts){
        if(excepts.isEmpty()) return randomManager(field);
        Object candidate = randomManager(field);

        while(excepts.contains(candidate)){
            candidate = randomManager(field);
        }

        return candidate;
    }

    private static Object randomManager(String field){
        switch(field){
            case "hospital":
                return randomHospital();
            case "manufacturer":
                return randomManufacturer();
            case "model":
                return randomModel();
            case "firmware_version":
                return randomFirmwareVersion();
            case  "device_type":
                return randomDeviceType();
            case "speciality":
                return randomMedicalSpeciality();
            case "category":
                return randomDeviceCategory();
            case "active_status":
                return randomActiveStatus();
            case "last_sync_time":
                return randomLastSyncTime();
            case "last_service_date":
                return randomLastServiceDate();
            case "warranty_expiry_date":
                return randomWarrantyExpiryDate();
            case "production_date":
                return randomProductionDate();
            case "usage_hours":
                return randomUsageHours();
            case "battery_level":
                return randomBatteryLevel();
            case "sync_frequency_seconds":
                return randomSyncFrequencySeconds();
            default:
                return null;
        }
    }   

    private static Object randomExceptManager(String field, Object except){
        switch(field){
            case "hospital":
                return randomHospitalExcept(except);
            case "manufacturer":
                return randomManufacturerExcept(except);
            case "model":
                return randomModelExcept(except);
            case "firmware_version":
                return randomFirmwareVersionExcept(except);
            case "device_type":
                return randomDeviceTypeExcept(except);
            case "speciality":
                return randomMedicalSpecialityExcept(except);
            case "category":
                return randomDeviceCategoryExcept(except);
            case "active_status":
                return randomActiveStatusExcept(except);
            case "last_sync_time":
                return randomLastSyncTimeExcept(except);
            case "last_service_date":
                return randomLastServiceDateExcept(except);
            case "warranty_expiry_date":
                return randomWarrantyExpiryDateExcept(except);
            case "production_date":
                return randomProductionDateExcept(except);
            case "usage_hours":
                return randomUsageHoursExcept(except);
            case "battery_level":
                return randomBatteryLevelExcept(except);
            case "sync_frequency_seconds":
                return randomSyncFrequencySecondsExcept(except);
            default:
                return null;
        }
    }
    // random generators
    // reuses the Random instance
    private int randomInt(int min, int max) {
        return random.nextInt(max - min + 1) + min;
    }

    private boolean randomBool() {
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

    private String randomHospitalExcept(String except) {
        String hospital = "Hospital " + randomString(10);
        while(hospital.equals(except)) {
            hospital = "Hospital " + randomString(10);
        }
        return hospital;
    }

    private String randomManufacturer() {
        return "Manufacturer " + randomString(6);
    }
    private String randomManufacturerExcept(String except) {
        String manufacturer = "Manufacturer " + randomString(6);
        while(manufacturer.equals(except)) {
            manufacturer = "Manufacturer " + randomString(6);
        }
        return manufacturer;
    }

    private String randomModel() {
        return "Model " + randomString(3);
    }
    private String randomModelExcept(String except) {
        String model = "Model " + randomString(3);
        while(model.equals(except)) {
            model = "Model " + randomString(3);
        }
        return model;
    }

    private String randomFirmwareVersion() {
        return "Firmware " + randomString(4);
    }
    private String randomFirmwareVersionExcept(String except) {
        String firmware = "Firmware " + randomString(4);
        while(firmware.equals(except)) {
            firmware = "Firmware " + randomString(4);
        }
        return firmware;
    }

    private String randomDeviceType() {
        return "Type " + randomString(5);
    }
    private String randomDeviceTypeExcept(String except) {
        String deviceType = "Type " + randomString(5);
        while(deviceType.equals(except)) {
            deviceType = "Type " + randomString(5);
        }
        return deviceType;
    }

    // values is default java method for enums
    private MedicalSpeciality randomMedicalSpeciality() {
        return MedicalSpeciality.values()[random.nextInt(MedicalSpeciality.values().length)];
    }
    private MedicalSpeciality randomMedicalSpecialityExcept(MedicalSpeciality except) {
        MedicalSpeciality speciality = MedicalSpeciality.values()[random.nextInt(MedicalSpeciality.values().length)];
        while(speciality.equals(except)) {
            speciality = MedicalSpeciality.values()[random.nextInt(MedicalSpeciality.values().length)];
        }
        return speciality;
    }

    private DeviceCategory randomDeviceCategory() {
        return DeviceCategory.values()[random.nextInt(DeviceCategory.values().length)];
    }
    private DeviceCategory randomDeviceCategoryExcept(DeviceCategory except) {
        DeviceCategory category = DeviceCategory.values()[random.nextInt(DeviceCategory.values().length)];
        while(category.equals(except)) {
            category = DeviceCategory.values()[random.nextInt(DeviceCategory.values().length)];
        }
        return category;
    }

    private boolean randomActiveStatus() {
        return random.nextBoolean();
    }  
    private boolean randomActiveStatusExcept(boolean except) {
        boolean activeStatus = random.nextBoolean();
        if(activeStatus == except) activeStatus = !activeStatus;
        return activeStatus;
    }

    private Timestamp randomLastSyncTime() {
        return randomPastTimestamp(base, 31);
    }
    private Timestamp randomLastSyncTimeExcept(Timestamp except) {
        Timestamp lastSyncTime = randomPastTimestamp(base, 31);
        while(lastSyncTime.equals(except)) {
            lastSyncTime = randomPastTimestamp(base, 31);
        }
        return lastSyncTime;
    }

    private int randomUsageHours() {
        return random.nextInt(10000);
    }

    private int randomUsageHoursExcept(int except) {
        int usageHours = random.nextInt(10000);
        while(usageHours == except) {
            usageHours = random.nextInt(10000);
        }
        return usageHours;
    }

    private Timestamp randomLastServiceDate() {
        return randomPastTimestamp(base, 31);
    }

    private Timestamp randomLastServiceDateExcept(Timestamp except) {   
        Timestamp lastServiceDate = randomPastTimestamp(base, 31);
        while(lastServiceDate.equals(except)) {
            lastServiceDate = randomPastTimestamp(base, 31);
        }
        return lastServiceDate;
    }

    private Timestamp randomWarrantyExpiryDate() {
        return randomFutureTimestamp(base, 31);
    }
    private Timestamp randomWarrantyExpiryDateExcept(Timestamp except) {
        Timestamp warrantyExpiryDate = randomFutureTimestamp(base, 31);
        while(warrantyExpiryDate.equals(except)) {
            warrantyExpiryDate = randomFutureTimestamp(base, 31);
        }
        return warrantyExpiryDate;
    }

    private Timestamp randomProductionDate() {
        return randomPastTimestamp(base, 31);
    }
    private Timestamp randomProductionDateExcept(Timestamp except) {
        Timestamp productionDate = randomPastTimestamp(base, 31);
        while(productionDate.equals(except)) {
            productionDate = randomPastTimestamp(base, 31);
        }
        return productionDate;
    }

    private int randomBatteryLevel() {
        return random.nextInt(100);
    }   

    private int randomBatteryLevelExcept(int except) {
        int batteryLevel = random.nextInt(100);
        while(batteryLevel == except) {
            batteryLevel = random.nextInt(100);
        }
        return batteryLevel;
    }

    private int randomSyncFrequencySeconds() {
        return random.nextInt(3600);
    }

    private int randomSyncFrequencySecondsExcept(int except) {
        int syncFrequencySeconds = random.nextInt(3600);
        while(syncFrequencySeconds == except) {
            syncFrequencySeconds = random.nextInt(3600);
        }
        return syncFrequencySeconds;    
    }
}