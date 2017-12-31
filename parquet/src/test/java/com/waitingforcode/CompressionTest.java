package com.waitingforcode;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class CompressionTest {

    private static final CodecFactory CODEC_FACTORY = new CodecFactory(new Configuration(), 1024);
    private static final CodecFactory.BytesCompressor GZIP_COMPRESSOR =
            CODEC_FACTORY.getCompressor(CompressionCodecName.GZIP);
    private static final CodecFactory.BytesCompressor SNAPPY_COMPRESSOR =
            CODEC_FACTORY.getCompressor(CompressionCodecName.SNAPPY);

    private static final BytesInput BIG_INPUT_INTS;
    static {
        List<BytesInput> first4000Numbers =
                IntStream.range(1, 4000).boxed().map(number -> BytesInput.fromInt(number)).collect(Collectors.toList());
        BIG_INPUT_INTS = BytesInput.concat(first4000Numbers);
    }

    private static final BytesInput SMALL_INPUT_STRING = BytesInput.concat(
            BytesInput.from(getUtf8("test")), BytesInput.from(getUtf8("test1")), BytesInput.from(getUtf8("test2")),
            BytesInput.from(getUtf8("test3")), BytesInput.from(getUtf8("test4"))
    );

    private static final BytesInput INPUT_DATA_STRING;
    private static final BytesInput INPUT_DATA_STRING_DISTINCT_WORDS;
    static {
        URL url = Resources.getResource("lorem_ipsum.txt");
        try {
            String text = Resources.toString(url, Charset.forName("UTF-8"));
            INPUT_DATA_STRING = BytesInput.from(getUtf8(text));
            String textWithDistinctWords =  Stream.of(text.split(" ")).distinct().collect(Collectors.joining(", "));
            INPUT_DATA_STRING_DISTINCT_WORDS = BytesInput.from(getUtf8(textWithDistinctWords));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void should_compress_lorem_ipsum_more_efficiently_with_gzip_than_without_compression() throws IOException {
        BytesInput compressedLorem = GZIP_COMPRESSOR.compress(INPUT_DATA_STRING);

        System.out.println("Compressed size = "+compressedLorem.size() + " vs plain size = "+INPUT_DATA_STRING.size());
        assertThat(compressedLorem.size()).isLessThan(INPUT_DATA_STRING.size());
    }

    @Test
    public void should_compress_text_without_repetitions_more_efficiently_with_gzip_than_without_compression() throws IOException {
        BytesInput compressedLorem = GZIP_COMPRESSOR.compress(INPUT_DATA_STRING_DISTINCT_WORDS);

        System.out.println("Compressed size = "+compressedLorem.size() +
                " vs plain size = "+INPUT_DATA_STRING_DISTINCT_WORDS.size());
        assertThat(compressedLorem.size()).isLessThan(INPUT_DATA_STRING_DISTINCT_WORDS.size());
    }

    @Test
    public void should_compress_small_text_less_efficiently_with_gzip_than_without_compression() throws IOException {
        BytesInput compressedLorem = GZIP_COMPRESSOR.compress(SMALL_INPUT_STRING);

        // Compressed size is greater because GZIP adds some additional compression metadata as, header
        // It can be seen in java.util.zip.GZIPOutputStream.writeHeader()
        System.out.println("Compressed size = "+compressedLorem.size() + " vs plain size = "+SMALL_INPUT_STRING.size());
        assertThat(compressedLorem.size()).isGreaterThan(SMALL_INPUT_STRING.size());
    }

    @Test
    public void should_compress_ints_more_efficiently_with_gzip_than_without_compression() throws IOException {
        BytesInput compressedLorem = GZIP_COMPRESSOR.compress(BIG_INPUT_INTS);

        System.out.println("Compressed size = "+compressedLorem.size() + " vs plain size = "+ BIG_INPUT_INTS.size());
        assertThat(compressedLorem.size()).isLessThan(BIG_INPUT_INTS.size());
    }

    @Test
    public void should_compress_lorem_ipsum_more_efficiently_with_snappy_than_without_compression() throws IOException {
        BytesInput compressedLorem = SNAPPY_COMPRESSOR.compress(INPUT_DATA_STRING);

        System.out.println("Compressed size = "+compressedLorem.size() + " vs plain size = "+INPUT_DATA_STRING.size());
        assertThat(compressedLorem.size()).isLessThan(INPUT_DATA_STRING.size());
    }

    @Test
    public void should_compress_text_without_repetitions_more_efficiently_with_snappy_than_without_compression() throws IOException {
        BytesInput compressedLorem = SNAPPY_COMPRESSOR.compress(INPUT_DATA_STRING_DISTINCT_WORDS);

        System.out.println("Compressed size = "+compressedLorem.size() +
                " vs plain size = "+INPUT_DATA_STRING_DISTINCT_WORDS.size());
        assertThat(compressedLorem.size()).isLessThan(INPUT_DATA_STRING_DISTINCT_WORDS.size());
    }

    @Test
    public void should_compress_small_text_less_efficiently_with_snappy_than_without_compression() throws IOException {
        BytesInput compressedLorem = SNAPPY_COMPRESSOR.compress(SMALL_INPUT_STRING);

        // For snappy the difference is much less smaller than in the case of Gzip  (1 byte)
        // The difference comes from the fact that Snappy stores the length of compressed output after
        // the compressed values
        // You can see that in the Snappy implementation C++ header file:
        // https://github.com/google/snappy/blob/32d6d7d8a2ef328a2ee1dd40f072e21f4983ebda/snappy.h#L111
        System.out.println("Compressed size = "+compressedLorem.size() + " vs plain size = "+SMALL_INPUT_STRING.size());
        assertThat(compressedLorem.size()).isGreaterThan(SMALL_INPUT_STRING.size());
    }

    @Test
    public void should_compress_ints_more_efficiently_with_snappy_than_without_compression() throws IOException {
        BytesInput compressedLorem = SNAPPY_COMPRESSOR.compress(BIG_INPUT_INTS);

        // Snappy is only slightly different than the plain encoding (1 byte less)
        System.out.println("Compressed size = "+compressedLorem.size() + " vs plain size = "+ BIG_INPUT_INTS.size());
        assertThat(compressedLorem.size()).isLessThan(BIG_INPUT_INTS.size());
    }

    @Test
    public void should_fail_compress_with_lzo_when_the_native_library_is_not_loaded() throws IOException {
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> CODEC_FACTORY.getCompressor(CompressionCodecName.LZO))
                .withMessageContaining("native-lzo library not available");
    }

    private static final byte[] getUtf8(String text) {
        try {
            return text.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

}

