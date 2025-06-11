package nl.medtechchain.chaincode.service.query;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import nl.medtechchain.proto.devicedata.DeviceDataAsset;
import nl.medtechchain.proto.devicedata.MedicalSpeciality;
import nl.medtechchain.proto.query.Filter;

class FilterServiceTest {

    private TestDataGenerator generator;

    @BeforeEach
    void setUp() {
        generator = new TestDataGenerator(42); // Use any seed for reproducibility
    }

    // ================ Integer Filter Tests ================

    // method to easily generate a plain integer asset
    private DeviceDataAsset createPlainAsset(int value) {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        usageHours.put(value, 1);
        spec.put("usage_hours", usageHours);
        return generator.generateAssetsWithCounts(spec, 1).get(0);
    }

    // method to easily generate an encrypted asset for integers
    private DeviceDataAsset createEncryptedAsset(long value, String version, TestEncryptionService encryptionService) {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> usageHours = new HashMap<>();
        var ciphertext = TestEncryptionService.encryptLong(value, version);
        usageHours.put(ciphertext, 1);
        spec.put("usage_hours", usageHours);
        return generator.generateAssetsWithCounts(spec, 1).get(0);
    }

    // a method to easily build an integer filter
    private Filter.IntegerFilter buildIntFilter(Filter.IntegerFilter.IntOperator op, long val) {
        return Filter.IntegerFilter.newBuilder()
                .setOperator(op)
                .setValue(val)
                .build();
    }

    // a method that helps to build a filter for usage hours
    private Filter buildUsageHoursFilter(Filter.IntegerFilter intFilter) {
        return Filter.newBuilder()
                .setField("usage_hours")
                .setIntegerFilter(intFilter)
                .build();
    }

    private TestEncryptionService createEncryptionService(Set<String> versions, String currentVersion) {
        return new TestEncryptionService(true, false, versions, currentVersion);
    }

    @Test
    void testPlainIntegerField_Equals() {
        DeviceDataAsset asset = createPlainAsset(42);
        FilterService service = new FilterService();
        // True
        assertTrue(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.EQUALS, 42))));
        // False
        assertFalse(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.EQUALS, 45))));
    }

    @Test
    void testEncryptedIntegerField_Equals() {
        TestEncryptionService encryptionService = createEncryptionService(Set.of("test-v1"), "test-v1");
        DeviceDataAsset asset = createEncryptedAsset(99L, encryptionService.getCurrentVersion(), encryptionService);
        FilterService service = new FilterService(encryptionService);
        // True
        assertTrue(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.EQUALS, 99))));
        // False
        assertFalse(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.EQUALS, 32))));
    }

    @Test
    void testPlainIntegerField_LessThan() {
        DeviceDataAsset asset = createPlainAsset(50);
        FilterService service = new FilterService();
        assertTrue(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.LESS_THAN, 100))));
        assertFalse(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.LESS_THAN, 10))));
    }

    @Test
    void testPlainIntegerField_GreaterThan() {
        DeviceDataAsset asset = createPlainAsset(150);
        FilterService service = new FilterService();
        assertTrue(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.GREATER_THAN, 100))));
        assertFalse(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.GREATER_THAN, 200))));
    }

    @Test
    void testPlainIntegerField_LessThanOrEqual() {
        DeviceDataAsset asset = createPlainAsset(100);
        FilterService service = new FilterService();
        assertTrue(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.LESS_THAN_OR_EQUAL, 100))));
        assertTrue(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.LESS_THAN_OR_EQUAL, 101))));
        assertFalse(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.LESS_THAN_OR_EQUAL, 99))));
    }

    @Test
    void testPlainIntegerField_GreaterThanOrEqual() {
        DeviceDataAsset asset = createPlainAsset(100);
        FilterService service = new FilterService();
        assertTrue(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.GREATER_THAN_OR_EQUAL, 100))));
        assertTrue(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.GREATER_THAN_OR_EQUAL, 99))));
        assertFalse(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.GREATER_THAN_OR_EQUAL, 101))));
    }

    @Test
    void testEncryptedIntegerField_LessThan() {
        TestEncryptionService encryptionService = createEncryptionService(Set.of("test-v1"), "test-v1");
        DeviceDataAsset asset = createEncryptedAsset(50L, encryptionService.getCurrentVersion(), encryptionService);
        FilterService service = new FilterService(encryptionService);
        assertTrue(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.LESS_THAN, 100))));
        assertFalse(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.LESS_THAN, 10))));
    }

    @Test
    void testEncryptedIntegerField_GreaterThan() {
        TestEncryptionService encryptionService = createEncryptionService(Set.of("test-v1"), "test-v1");
        DeviceDataAsset asset = createEncryptedAsset(150L, encryptionService.getCurrentVersion(), encryptionService);
        FilterService service = new FilterService(encryptionService);
        assertTrue(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.GREATER_THAN, 100))));
        assertFalse(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.GREATER_THAN, 200))));
    }

    @Test
    void testEncryptedIntegerField_LessThanOrEqual() {
        TestEncryptionService encryptionService = createEncryptionService(Set.of("test-v1"), "test-v1");
        DeviceDataAsset asset = createEncryptedAsset(100L, encryptionService.getCurrentVersion(), encryptionService);
        FilterService service = new FilterService(encryptionService);
        assertTrue(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.LESS_THAN_OR_EQUAL, 100))));
        assertTrue(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.LESS_THAN_OR_EQUAL, 101))));
        assertFalse(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.LESS_THAN_OR_EQUAL, 99))));
    }

    @Test
    void testEncryptedIntegerField_GreaterThanOrEqual() {
        TestEncryptionService encryptionService = createEncryptionService(Set.of("test-v1"), "test-v1");
        DeviceDataAsset asset = createEncryptedAsset(100L, encryptionService.getCurrentVersion(), encryptionService);
        FilterService service = new FilterService(encryptionService);
        assertTrue(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.GREATER_THAN_OR_EQUAL, 100))));
        assertTrue(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.GREATER_THAN_OR_EQUAL, 99))));
        assertFalse(service.checkFilter(asset, buildUsageHoursFilter(buildIntFilter(Filter.IntegerFilter.IntOperator.GREATER_THAN_OR_EQUAL, 101))));
    }

    @Test
    void testEncryptedIntegerField_DifferentKeyVersions() {
        Set<String> versions = Set.of("paillier-v1", "paillier-v2");
        TestEncryptionService encryptionService = createEncryptionService(versions, "paillier-v2");
        DeviceDataAsset assetV1 = createEncryptedAsset(123L, "paillier-v1", encryptionService);
        DeviceDataAsset assetV2 = createEncryptedAsset(125L, "paillier-v2", encryptionService);
        FilterService service = new FilterService(encryptionService);
        Filter.IntegerFilter intFilter = buildIntFilter(Filter.IntegerFilter.IntOperator.EQUALS, 123);
        Filter filter = buildUsageHoursFilter(intFilter);
        assertTrue(service.checkFilter(assetV1, filter), "Should match asset with paillier-v1");
        assertFalse(service.checkFilter(assetV2, filter), "Should not match asset with paillier-v2");
    }

    // ================ String Filter Tests ================

    // method to easily generate an encrypted asset for strings
    private DeviceDataAsset createEncryptedStringAsset(String value, String version, TestEncryptionService encryptionService) {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> manufacturer = new HashMap<>();
        var ciphertext = TestEncryptionService.encryptString(value, version);
        manufacturer.put(ciphertext, 1);
        spec.put("manufacturer", manufacturer);
        return generator.generateAssetsWithCounts(spec, 1).get(0);
    }

    // a method to easily build a string filter
    private Filter.StringFilter buildStrFilter(Filter.StringFilter.StringOperator op, String val) {
        return Filter.StringFilter.newBuilder()
                .setOperator(op)
                .setValue(val)
                .build();
    }

    // a method that helps to build a filter for speciality
    private Filter buildManufactorerFilter(Filter.StringFilter strFilter) {
        return Filter.newBuilder()
                .setField("manufacturer")
                .setStringFilter(strFilter)
                .build();
    }

    @Test
    void testEncryptedStringField_DifferentKeyVersions() {
        Set<String> versions = Set.of("paillier-v1", "paillier-v2");
        TestEncryptionService encryptionService = createEncryptionService(versions, "paillier-v2");
        DeviceDataAsset assetV1 = createEncryptedStringAsset("string_one", "paillier-v1", encryptionService);
        DeviceDataAsset assetV2 = createEncryptedStringAsset("string_two", "paillier-v2", encryptionService);
        FilterService service = new FilterService(encryptionService);
        Filter.StringFilter strFilter = buildStrFilter(Filter.StringFilter.StringOperator.EQUALS, "string_one");
        Filter filter = buildManufactorerFilter(strFilter);
        assertTrue(service.checkFilter(assetV1, filter), "Should match asset with paillier-v1");
        assertFalse(service.checkFilter(assetV2, filter), "Should not match asset with paillier-v2");
    }


    // ================ Enum Filter Tests ================


    // method to easily generate an encrypted asset for enums
    private DeviceDataAsset createEncryptedEnumAsset(String value, String version, TestEncryptionService encryptionService) {
        Map<String, Map<Object, Integer>> spec = new HashMap<>();
        Map<Object, Integer> speciality = new HashMap<>();
        var ciphertext = TestEncryptionService.encryptLong(MedicalSpeciality.valueOf(value).getNumber(), version);
        speciality.put(ciphertext, 1);
        spec.put("speciality", speciality);
        return generator.generateAssetsWithCounts(spec, 1).get(0);
    }
    
    // a method to easily build an enum filter
    private Filter.EnumFilter buildEnumFilter(String val) {
        return Filter.EnumFilter.newBuilder()
                .setValue(val)
                .build();
    }

    // a method that helps to build a filter for speciality
    private Filter buildSpecialityFilter(Filter.EnumFilter enumFilter) {
        return Filter.newBuilder()
                .setField("speciality")
                .setEnumFilter(enumFilter)
                .build();
    }

    @Test
    void testEncryptedEnumField_DifferentKeyVersions() {
        Set<String> versions = Set.of("paillier-v1", "paillier-v2");
        TestEncryptionService encryptionService = createEncryptionService(versions, "paillier-v2");
        DeviceDataAsset assetV1 = createEncryptedEnumAsset("DERMATOLOGY", "paillier-v1", encryptionService);
        DeviceDataAsset assetV2 = createEncryptedEnumAsset("CARDIOLOGY", "paillier-v2", encryptionService);
        FilterService service = new FilterService(encryptionService);
        Filter.EnumFilter enumFilter = buildEnumFilter("DERMATOLOGY");
        Filter filter = buildSpecialityFilter(enumFilter);
        assertTrue(service.checkFilter(assetV1, filter), "Should match asset with paillier-v1");
        assertFalse(service.checkFilter(assetV2, filter), "Should not match asset with paillier-v2");
    }
}