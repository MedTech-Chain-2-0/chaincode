package nl.medtechchain.chaincode.test;

import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.auto.value.extension.toprettystring.ToPrettyString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestDataGeneratorTests {

    // correct number of assets
    @Test
    public void sizeTest1() {
        TestDataGenerator generator = new TestDataGenerator();
        int n = 10;
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssets(new HashMap<>(), n);
        Assertions.assertEquals(n, assets.size(), "Generated assets have different size from specified");
    }

    @Test 
    public void sizeTest2() {
        TestDataGenerator generator = new TestDataGenerator();
        int n = -1;
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssets(new HashMap<>(), n);
        Assertions.assertEquals(0, assets.size(), "Generated assets have different size from specified");    
    }

    // 2. Field-override check
    @Test
    public void overrideTest1() {
        TestDataGenerator generator = new TestDataGenerator();
        String expectedHospital = "hospital A";

        Map<String, Object> overrides = new HashMap<>();
        overrides.put("hospital", expectedHospital);

        List<DeviceDataAsset> assets = generator.generateDeviceDataAssets(overrides, 5);

        for (DeviceDataAsset asset : assets) {
            // each hospital is expected
            String hospital = asset.getDeviceData().getHospital().getPlain();
            Assertions.assertEquals(expectedHospital, hospital, "Hospital field different from expected");
        }
    }

    // fixed value counts
    @Test
    public void countTest1() {
        TestDataGenerator generator = new TestDataGenerator();

        Map<Object, Integer> countMap = new HashMap<>();
        countMap.put(50, 3);
        countMap.put(60, 1);
        countMap.put(30, 4);


        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("battery_level", countMap);

        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 1);

        Assertions.assertEquals(3 + 1 + 4, assets.size(), "Total number of assets is werid");
        int fifthy = 0;
        int sixty = 0;
        int thirty = 0;
        for(DeviceDataAsset asset : assets){
            if(asset.getDeviceData().getBatteryLevel().getPlain() == 50) fifthy++;
            if(asset.getDeviceData().getBatteryLevel().getPlain() == 60) sixty++;
            if(asset.getDeviceData().getBatteryLevel().getPlain() == 30) thirty++;
        }
        Assertions.assertEquals(3, fifthy, "Bettery level count wrong");
        Assertions.assertEquals(1, sixty, "Bettery level count wrong");
        Assertions.assertEquals(4, thirty, "Bettery level count wrong");
    }

    // method fills up assets up until n
    @Test
    public void countTest2() {
        TestDataGenerator generator = new TestDataGenerator();
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> countMap = new HashMap<>();
        countMap.put(50, 3);
        countMap.put(60, 1);
        countMap.put(30, 4);
        spec.put("battery_level", countMap);
        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 12);
        Assertions.assertEquals(12, assets.size(), "Total number of assets is werid");
    }

    // seeds
    @Test
    public void seedsTest1() {
        long seed = 12L;
        int n = 12;

        TestDataGenerator gen1 = new TestDataGenerator(seed);
        TestDataGenerator gen2 = new TestDataGenerator(seed);
        List<DeviceDataAsset> assets1 = gen1.generateDeviceDataAssets(new HashMap<>(), n);
        List<DeviceDataAsset> assets2 = gen2.generateDeviceDataAssets(new HashMap<>(), n);
        
        for (int i = 0; i < n; i++) {
            DeviceDataAsset a1 = assets1.get(i);
            DeviceDataAsset a2 = assets2.get(i);
            Assertions.assertTrue(a1.equals(a2), "Assets are not equal");
        }
    }

    @Test
    public void encryptedFieldTest1() {
        TestDataGenerator generator = new TestDataGenerator();
        TestDataGenerator.Cyphertext cypher = generator.new Cyphertext("latipsoh");

        Map<String, Object> overrides = new HashMap<>();
        overrides.put("hospital", cypher);

        List<DeviceDataAsset> assets = generator.generateDeviceDataAssets(overrides, 3);

        for (DeviceDataAsset asset : assets) {
            DeviceDataAsset.StringField hospital = asset.getDeviceData().getHospital();
            Assertions.assertTrue(hospital.hasEncrypted(), "field not encrypted");
            Assertions.assertFalse(hospital.hasPlain(), "field should not have plain value ");
            Assertions.assertEquals("latipsoh", hospital.getEncrypted(), "Encrypted mismatch");
        }
    }

    @Test
    public void encryptedFieldTest2() {
        TestDataGenerator generator = new TestDataGenerator();
        TestDataGenerator.Cyphertext cypher = generator.new Cyphertext("78");

        Map<String, Object> overrides = new HashMap<>();
        overrides.put("battery_level", cypher);

        List<DeviceDataAsset> assets = generator.generateDeviceDataAssets(overrides, 5);

        for (DeviceDataAsset asset : assets) {
            DeviceDataAsset.IntegerField battery = asset.getDeviceData().getBatteryLevel();
            Assertions.assertTrue(battery.hasEncrypted(), "field not encrypted");
            Assertions.assertFalse(battery.hasPlain(), "field should not have plain value");
            Assertions.assertEquals("78", battery.getEncrypted(), "Encrypted mismatch");
        }
    }
    
    @Test
    public void encryptedCountTest() {
        TestDataGenerator generator = new TestDataGenerator();
        TestDataGenerator.Cyphertext cypher = generator.new Cyphertext("78");

        Map<Object, Integer> countMap = new HashMap<>();
        countMap.put(cypher, 4);

        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        spec.put("battery_level", countMap);

        List<DeviceDataAsset> assets = generator.generateDeviceDataAssetsWithCount(spec, 4);

        Assertions.assertEquals(4, assets.size(), "Incorrect number of assets generated");

        for (DeviceDataAsset asset : assets) {
            DeviceDataAsset.IntegerField battery = asset.getDeviceData().getBatteryLevel();
            Assertions.assertTrue(battery.hasEncrypted(), "field not encrypted");
            Assertions.assertFalse(battery.hasPlain(), "field should not have plain value");
            Assertions.assertEquals("78", battery.getEncrypted(), "Encrypted mismatch");
        }
    }
}