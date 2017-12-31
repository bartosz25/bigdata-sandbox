package com.waitingforcode;

import avro.shaded.com.google.common.collect.Lists;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.values.bitpacking.BitPacking;
import org.apache.parquet.column.values.bitpacking.ByteBitPackingLE;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.io.api.Binary;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.parquet.io.api.Binary.fromString;
import static org.assertj.core.api.Assertions.assertThat;

public class EncodingTest {

    @Test
    public void should_store_repeatable_data_more_efficiently_than_in_plain_storage() throws IOException {
        DirectByteBufferAllocator bytesAllocator = DirectByteBufferAllocator.getInstance();
        int slabSize = 256;
        int pageSize = 1000;
        DeltaByteArrayWriter deltaByteArrayWriter = new DeltaByteArrayWriter(slabSize, pageSize, bytesAllocator);

        List<String> words = Lists.newArrayList("absorb", "absorption", "acceleration", "action", "ampere", "amplitude",
                "cadency", "cadent", "cadential", "cadet", "collision", "color", "colorfast", "colorful",
                "racketeering", "racketing", "rackets", "rackety");
        List<Binary> binaryWords = words.stream().map(word -> fromString(word)).collect(Collectors.toList());
        binaryWords.forEach(binaryWord -> deltaByteArrayWriter.writeBytes(binaryWord));
        String allWords = words.stream().reduce(String::concat).get();

        byte[] encodedBytes = deltaByteArrayWriter.getBytes().toByteArray();
        byte[] allWordsBytes = allWords.getBytes("UTF-8");
        // As you can see, the DeltaByteArray is more efficient in the case when the letters are dictionary-like
        // However, you can go to the test should_store_non_repeatable_data_less_efficiently_than_in_plain_storage
        // to see what happens if the data is not dictionary-like
        assertThat(encodedBytes).hasSize(138);
        assertThat(allWordsBytes).hasSize(142);
    }

    @Test
    public void should_store_non_repeatable_data_less_efficiently_than_in_plain_storage() throws IOException {
        DirectByteBufferAllocator bytesAllocator = DirectByteBufferAllocator.getInstance();
        int slabSize = 256;
        int pageSize = 1000;
        DeltaByteArrayWriter deltaByteArrayWriter = new DeltaByteArrayWriter(slabSize, pageSize, bytesAllocator);

        List<String> words = Lists.newArrayList("absorb", "acceleration", "ampere",
                "cadency",  "collision", "racketeering", "sad", "sale", "sanction");
        List<Binary> binaryWords = words.stream().map(word -> fromString(word)).collect(Collectors.toList());
        binaryWords.forEach(binaryWord -> deltaByteArrayWriter.writeBytes(binaryWord));
        String allWords = words.stream().reduce(String::concat).get();

        byte[] encodedBytes = deltaByteArrayWriter.getBytes().toByteArray();
        byte[] allWordsBytes = allWords.getBytes("UTF-8");
        // Here the data is less dictionary-like, so the encoding should be worse
        assertThat(encodedBytes).hasSize(104);
        assertThat(allWordsBytes).hasSize(67);
    }

    @Test
    public void should_store_ints_with_rle_hybrid_encoder_more_efficiently_than_in_plain_storage() throws IOException {
        DirectByteBufferAllocator bytesAllocator = DirectByteBufferAllocator.getInstance();
        int initialCapacity = 256;
        int pageSize = 1000;
        int bitWidth = 4;
        RunLengthBitPackingHybridEncoder runLengthBitPackingHybridEncoder =
                new RunLengthBitPackingHybridEncoder(bitWidth, initialCapacity, pageSize, bytesAllocator);

        List<Integer> numbers = IntStream.range(0, 100).boxed().collect(Collectors.toList());
        List<Byte> plainNumberBytes = new ArrayList<>();
        for (int i = 0; i < numbers.size(); i++) {
            runLengthBitPackingHybridEncoder.writeInt(numbers.get(i));
            plainNumberBytes.add(numbers.get(i).byteValue());
        }
        byte[] encodedBytes = runLengthBitPackingHybridEncoder.toBytes().toByteArray();
        assertThat(encodedBytes).hasSize(53);
        assertThat(plainNumberBytes).hasSize(100);
    }

    @Test
    public void should_apply_either_rle_or_bit_packing_for_values_of_different_characteristics() throws IOException {
        DirectByteBufferAllocator bytesAllocator = DirectByteBufferAllocator.getInstance();
        int initialCapacity = 256;
        int pageSize = 1000;
        int bitWidth = 4;
        RunLengthBitPackingHybridEncoder runLengthBitPackingHybridEncoder =
                new RunLengthBitPackingHybridEncoder(bitWidth, initialCapacity, pageSize, bytesAllocator);

        // RLE is written after 8 repeated values
        int repeatedValue = 3;
        for(int iteration = 0; iteration < 9; iteration++) {
            runLengthBitPackingHybridEncoder.writeInt(repeatedValue);
        }
        // Otherwise the values are bit-packed
        int bitPackedValue1 = 0;
        int bitPackedValue2 = 1;
        for (int iteration = 0; iteration < 4; iteration++) {
            runLengthBitPackingHybridEncoder.writeInt(bitPackedValue1);
            runLengthBitPackingHybridEncoder.writeInt(bitPackedValue2);
        }

        byte[] encodedBytes = runLengthBitPackingHybridEncoder.toBytes().toByteArray();
        String encodedBytesRepresentation = stringifyBytes(encodedBytes);
        // In received results:
        // * 00010010 - 1 bit left shifted number of repetitions (18, after >> 1 it gives 9 repetitions)
        // * 00000101 - repeated value (3 in that case)
        // * the next values represent bit packed 0,1,0,1,0,1,0,1 pairs
        assertThat(encodedBytes).hasSize(7);
        assertThat(encodedBytesRepresentation).isEqualTo("00010010 00000011 00000011 00010000 00010000 00010000 00010000");
    }

    @Test
    public void should_store_small_variations_with_delta_encoding_more_efficiently_than_in_plain_storage() throws IOException {
        DirectByteBufferAllocator bytesAllocator = DirectByteBufferAllocator.getInstance();
        int slabSize = 256;
        int pageSize = 1000;
        DeltaBinaryPackingValuesWriterForInteger deltaBinaryPackingValuesWriterForInteger =
                new DeltaBinaryPackingValuesWriterForInteger(slabSize, pageSize, bytesAllocator);

        List<Integer> numbers = IntStream.range(0, 100).boxed().collect(Collectors.toList());
        List<Byte> plainNumberBytes = new ArrayList<>();
        for (int i = 0; i < numbers.size(); i++) {
            deltaBinaryPackingValuesWriterForInteger.writeInteger(numbers.get(i));
            plainNumberBytes.add(numbers.get(i).byteValue());
        }
        byte[] encodedBytes = deltaBinaryPackingValuesWriterForInteger.getBytes().toByteArray();
        assertThat(encodedBytes).hasSize(10);
        // 10 times less space is needed for delta encoding with auto-incremented values
        // Even if the variation is bigger, the storage is still efficient
        assertThat(plainNumberBytes).hasSize(100);
    }

    @Test
    public void should_store_big_variations_with_delta_encoding_more_efficiently_than_in_plain_storage() throws IOException {
        DirectByteBufferAllocator bytesAllocator = DirectByteBufferAllocator.getInstance();
        int slabSize = 256;
        int pageSize = 1000;
        DeltaBinaryPackingValuesWriterForInteger deltaBinaryPackingValuesWriterForInteger =
                new DeltaBinaryPackingValuesWriterForInteger(slabSize, pageSize, bytesAllocator);

        List<Integer> numbers = IntStream.range(0, 100).boxed().collect(Collectors.toList());
        List<Byte> plainNumberBytes = new ArrayList<>();
        for (int i = 0; i < numbers.size(); i++) {
            Integer multipleOf3000 = numbers.get(i)*3000;
            deltaBinaryPackingValuesWriterForInteger.writeInteger(multipleOf3000);
            plainNumberBytes.add(multipleOf3000.byteValue());
        }
        byte[] encodedBytes = deltaBinaryPackingValuesWriterForInteger.getBytes().toByteArray();
        assertThat(encodedBytes).hasSize(11);
        assertThat(plainNumberBytes).hasSize(100);
    }

    @Test
    public void should_store_ints_with_deprecated_bit_packing() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        int intLengthInBits = 3;
        BitPacking.BitPackingWriter bitPackingWriter = BitPacking.getBitPackingWriter(intLengthInBits, byteArrayOutputStream);

        List<Integer> numbers = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7);

        for (int i = 0; i < numbers.size(); i++) {
            bitPackingWriter.write(numbers.get(i));
        }
        bitPackingWriter.finish();

        byte[] encodedBytes = byteArrayOutputStream.toByteArray();
        // 8 int32 number, represented as 3 bits are finally written with only 3 bytes
        // instead of 8 bytes if we'd use the plain encoding
        assertThat(encodedBytes).hasSize(3);
        String encodedBytesRepresentation = stringifyBytes(encodedBytes);
        // This bit packing corresponds to the deprecated version - the values are packed from the most significant bit
        // to the least significant bit; So in our case they're packed back to back
        assertThat(encodedBytesRepresentation).isEqualTo("00000101 00111001 01110111");
    }

    @Test
    public void should_not_change_int_storage_with_deprecated_8_bitwidth() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        int intLengthInBits = 8;
        BitPacking.BitPackingWriter bitPackingWriter = BitPacking.getBitPackingWriter(intLengthInBits, byteArrayOutputStream);

        List<Integer> numbers = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7);

        for (int i = 0; i < numbers.size(); i++) {
            bitPackingWriter.write(numbers.get(i));
        }
        bitPackingWriter.finish();

        byte[] encodedBytes = byteArrayOutputStream.toByteArray();
        // Here the ints are represented as in plain encoding, so it doesn't bring a lot of advantages here
        assertThat(encodedBytes).hasSize(8);
    }

    @Test
    public void should_store_small_number_of_ints_with_rle_bit_packing_approach() throws IOException {
        int bitWidth = 3;
        BytePacker bytePacker = ByteBitPackingLE.factory.newBytePacker(bitWidth);
        byte[] outputValues = new byte[bitWidth];
        int[] inputValues = new int[] {0, 1, 2, 3, 4, 5, 6, 7};
        int startPosition = 0;
        int outputPosition = 0;

        bytePacker.pack8Values(inputValues, startPosition, outputValues, outputPosition);

        String encodedBytesRepresentation = stringifyBytes(outputValues);
        // If you compare the result with should_store_ints_with_deprecated_bit_packing
        // you will see that the bits are not encoded in the same logic
        assertThat(encodedBytesRepresentation).isEqualTo("10001000 11000110 11111010");
    }

    @Test
    public void should_store_plain_encoded_values_with_defined_rule_of_32_bytes() throws IOException {
        DirectByteBufferAllocator bytesAllocator = DirectByteBufferAllocator.getInstance();
        int initialSize = 128;
        int pageSize = 1000;
        PlainValuesWriter plainValuesWriter = new PlainValuesWriter(initialSize, pageSize, bytesAllocator);
        List<Integer> numbers = IntStream.rangeClosed(1, 10).boxed().collect(Collectors.toList());
        for (int i = 0; i < numbers.size(); i++) {
            plainValuesWriter.writeInteger(numbers.get(i));
        }

        byte[] encodedBytes = plainValuesWriter.getBytes().toByteArray();
        String encodedBytesRepresentation = stringifyBytes(encodedBytes);
        System.out.println("Encoded ="+encodedBytesRepresentation);
        // 40 bytes should be used because we're writing int32 that takes
        // 4 bytes for every value
        assertThat(encodedBytes).hasSize(40);
    }

    @Test
    public void should_store_plain_encoded_texts_as_length_variable_arrays() throws IOException {
        DirectByteBufferAllocator bytesAllocator = DirectByteBufferAllocator.getInstance();
        int initialSize = 128;
        int pageSize = 1000;
        PlainValuesWriter plainValuesWriter = new PlainValuesWriter(initialSize, pageSize, bytesAllocator);
        plainValuesWriter.writeBytes(fromString("Amsterdam"));
        plainValuesWriter.writeBytes(fromString("Basel"));
        plainValuesWriter.writeBytes(fromString("Chicago"));
        plainValuesWriter.writeBytes(fromString("Dortmund"));

        byte[] encodedBytes = plainValuesWriter.getBytes().toByteArray();
        // for binary data, the following information is written:
        // * the length in 4 bytes
        // * the value
        // Thus it gives:
        // Amsterdam = 4 + 9 = 13
        // Basel = 4 + 5 = 9
        // Chicago = 4 + 7 = 11
        // Dortmund = 4 + 8 = 12
        // = 45
        assertThat(encodedBytes).hasSize(45);
    }

    private static String stringifyBytes(byte[] bytes) {
        StringBuilder stringBuilder = new StringBuilder();
        for(int i = 0; i < bytes.length; i++) {
            int oneByte = bytes[i];
            if (oneByte < 0) {
                oneByte = 256 + oneByte;
            }
            stringBuilder.append(stringifyNumber(oneByte)).append(" ");
        }
        return stringBuilder.toString().trim();
    }

    private static String stringifyNumber(int number) {
        StringBuilder stringBuilder = new StringBuilder();
        String numberBinaryRepresentation = Integer.toBinaryString(number);
        int representationLength = numberBinaryRepresentation.length();
        while (representationLength < 8) {
            stringBuilder.append("0");
            representationLength++;
        }
        stringBuilder.append(numberBinaryRepresentation);
        return stringBuilder.toString();
    }

}


