package org.apache.spark.ml;

// Examples of custom transformer
// https://github.com/SupunS/play-ground/blob/master/test.spark.client_2/src/main/java/MeanImputer.java
// https://github.com/SupunS/play-ground/blob/master/test.spark.client_2/src/main/java/RegexTransformer.java
public class WordNormalizer {}
/*extends Transformer implements Serializable , MLWritable, MLReadable {

    private static final String UDF_NAME = "WordNormalizer";

    private static final String NEW_FIELD_NAME = "normalizedFiltered";

    private final String id;

    private String inputCol;

    public WordNormalizer(String id) {
        this.id = id;
    }

    @Override
    public StructType transformSchema(StructType inputSchema) {
        int allFields = inputSchema.fields().length;
        StructField[] fields = new StructField[allFields+1];
        for (int i = 0; i < allFields; i++) {
            fields[i] = (inputSchema.fields()[i]);
        }
        fields[allFields] = getLetterField();
        return new StructType(fields);
    }

    private StructField getLetterField() {
        DataType fieldType = DataTypes.createArrayType(DataTypes.StringType);
        boolean nullable = false;
        Metadata metadata = Metadata.empty();
        return new StructField(NEW_FIELD_NAME, fieldType, nullable, metadata);
    }

    public WordNormalizer setInputCol(String inputCol) {
        this.inputCol = inputCol;
        return this;
    }

    public String getOutputCol() {
        return NEW_FIELD_NAME;
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        ArrayType returnedType = DataTypes.createArrayType(DataTypes.StringType);
        dataset.sqlContext().udf()
                .register(UDF_NAME, (WrappedArray<String> letters) -> {
                    Iterator<String> iterator = letters.iterator();
                    List<String> result = new ArrayList<>();
                    while (iterator.hasNext()) {
                        result.add(iterator.next().toUpperCase());
                    }
                    return result;
                }, returnedType);
        Column col = dataset.col(inputCol);
        col = functions.callUDF(UDF_NAME, col);
        return dataset.withColumn(NEW_FIELD_NAME, col);
    }

    @Override
    public Transformer copy(ParamMap extra) {
        return defaultCopy(extra);
    }

    @Override
    public String uid() {
        return id;
    }

    // Below methods are mandatory save/load model using this transformer
    @Override
    public MLWriter write() {
        return new DefaultParamsWriter(this);
    }

    @Override
    public void save(String path) throws IOException {
        write().save(path);
    }

    @Override
    public WordNormalizer load(String path) {
        System.out.println("Loading path " + path);
        return new DefaultParamsReader<WordNormalizer>().load(path);
    }

    @Override
    public MLReader<WordNormalizer> read() {
        System.out.println("Returning default params reader");
        return new DefaultParamsReader();
    }


}
                                                                        */