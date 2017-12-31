package com.waitingforcode;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import static org.apache.parquet.schema.OriginalType.*;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class DataTypesTest {

    @Test
    public void should_create_map_of_integer_string_pairs() {
        Type letter = Types.required(BINARY).as(UTF8).named("letter");
        Type number = Types.required(INT32).named("number");
        GroupType map = Types.buildGroup(REQUIRED).as(OriginalType.MAP).addFields(letter, number).named("numbers_letters");

        String stringRepresentation = getStringRepresentation(map);

        assertThat(stringRepresentation).isEqualTo("required group numbers_letters (MAP) {\n" +
                "  required binary letter (UTF8);\n" +
                "  required int32 number;\n" +
                "}");
    }

    @Test
    public void should_create_a_list() {
        Type letterField = Types.required(BINARY).as(UTF8).named("letter");
        GroupType lettersList = Types.buildGroup(REPEATED).as(OriginalType.LIST).addField(letterField).named("letters");

        String stringRepresentation = getStringRepresentation(lettersList);

        assertThat(stringRepresentation).isEqualTo("repeated group letters (LIST) {\n" +
                "  required binary letter (UTF8);\n" +
                "}");
    }

    @Test
    public void should_create_int96_type() {
        Type bigNumberField = Types.required(INT96).named("big_number");

        String stringRepresentation = getStringRepresentation(bigNumberField);

        assertThat(stringRepresentation).isEqualTo("required int96 big_number");
    }

    @Test
    public void should_create_boolean_type() {
        Type isPairFlagField = Types.required(BOOLEAN).named("is_pair");

        String stringRepresentation = getStringRepresentation(isPairFlagField);

        assertThat(stringRepresentation).isEqualTo("required boolean is_pair");
    }

    @Test
    public void should_fail_on_applying_complex_type_to_primitive_type() {
        assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> {
            Types.optional(FIXED_LEN_BYTE_ARRAY).length(10).as(MAP).named("letters");
        }).withMessageContaining("MAP can not be applied to a primitive type");
    }

    @Test
    public void should_create_fixed_length_array_type() {
        Type salary = Types.optional(FIXED_LEN_BYTE_ARRAY).length(10).precision(4).as(DECIMAL).named("salary");

        String stringRepresentation = getStringRepresentation(salary);

        assertThat(stringRepresentation).isEqualTo("optional fixed_len_byte_array(10) salary (DECIMAL(4,0))");
    }

    @Test
    public void should_create_simple_string_type() {
        Type textType = Types.required(BINARY).as(UTF8).named("text");

        String stringRepresentation = getStringRepresentation(textType);

        assertThat(stringRepresentation).isEqualTo("required binary text (UTF8)");
    }

    @Test
    public void should_create_complex_type() {
        // Parquet also allows the creation of "complex" (nested) types that are
        // similar to objects from object-oriented languages
        Type userType = Types.requiredGroup()
            .required(INT64).named("id")
            .required(BINARY).as(UTF8).named("email")
            .named("User");

        String stringRepresentation = getStringRepresentation(userType);

        assertThat(stringRepresentation).isEqualTo("required group User {\n" +
                "  required int64 id;\n" +
                "  required binary email (UTF8);\n" +
                "}");
    }

    private static String getStringRepresentation(Type type) {
        StringBuilder bigNumberStringBuilder = new StringBuilder();
        type.writeToStringBuilder(bigNumberStringBuilder, "");
        return type.toString();
    }

}
