package com.indeed.imhotep.index.builder.util;

/**
 * @author mmorrison
 */
public class EasyIndexField {
    // name of the key for this field in the log entries
    // this may be null, but if so, the index value should be created in the generate() callback
    public String logKey = null;
    public boolean logDecode = false;
    public boolean allowUppercase = false; // skip force lowercasing of values if set to true

    // name of the key to be dumped in the lucene index
    public String indexKey = null;

    // is this field a dimension for ramses? A dimension field must be an integer.
    // for each dimension field, the following will be created:
    //   an inverted-inverted-index (docId -> this metric)
    //   a line in the dimensions.desc file
    public boolean isDimension = false;
    public String dimensionType = null;
    public String dimensionUnits = null;
    private String dimensionKey = null;
    public EncodedValueParser dimensionValueParser = null;
    public EncodedValueHandler dimensionValueHandler = null;

    // should this value be tokenized in lucene?
    public boolean tokenized = false;

    // do not use any quotes or fancy characters in your description
    public String description = null;

    public EasyIndexField(String indexKey) {
        this.indexKey = indexKey;
    }
    public EasyIndexField(String indexKey, String desc) {
        this.indexKey = indexKey;
        this.description = desc;
    }

    public EasyIndexField hasDescription(String desc) {
        this.description = desc;
        return this;
    }

    // Indicates this field should be automatically parsed from the logs
    public EasyIndexField autoParse() {
        return autoParse(false);
    }
    public EasyIndexField autoParse(boolean decode) {
        this.logKey = this.indexKey;
        this.logDecode = decode;
        return this;
    }
    public EasyIndexField autoParse(String logKey) {
        this.logKey = logKey;
        return this;
    }
    public EasyIndexField autoParse(String logKey, boolean decode) {
        this.logKey = logKey;
        this.logDecode = decode;
        return this;
    }
    public EasyIndexField isTokenized() {
        this.tokenized = true;
        return this;
    }
    public EasyIndexField isUpperCase() {
        this.allowUppercase = true;
        return this;
    }
    public EasyIndexField isDimension() {
        this.isDimension = true;
        return this;
    }
    public EasyIndexField isDimension(EncodedValueParser dimensionValueParser) {
        if(this.dimensionValueHandler != null) {
            throw new IllegalStateException("EasyIndexField cannot have both a EncodedValueHandler and EncodedValueParser");
        }
        this.isDimension = true;
        this.dimensionValueParser = dimensionValueParser;
        return this;
    }
    public EasyIndexField isDimension(EncodedValueHandler dimensionValueHandler) {
        if(this.dimensionValueParser != null) {
            throw new IllegalStateException("EasyIndexField cannot have both a EncodedValueHandler and EncodedValueParser");
        }
        this.isDimension = true;
        this.dimensionValueHandler = dimensionValueHandler;

        return this;
    }
    public EasyIndexField isDimension(String units) {
        this.isDimension = true;
        this.dimensionUnits = units;
        return this;
    }
    public EasyIndexField isDimension(String units, String type) {
        this.isDimension = true;
        this.dimensionUnits = units;
        this.dimensionType = type;
        return this;
    }
    /**
     * For some reason, some old indexes have dimensions with names different from their index keys.
     * In these rare cases, specify the dimension name with this function.
     */
    public EasyIndexField dimKey(String key) {
        this.dimensionKey = key;
        return this;
    }

    /**
     * Get Functions
     */
    public String getDimensionKey() {
        if(dimensionKey != null)
            return dimensionKey;
        return indexKey;
    }

    /**
     * Helpers
     */
    public static void autoParseAll(EasyIndexField[] fields) {
        for(EasyIndexField field : fields) {
            if(field.logKey == null)
                field.autoParse();
        }
    }
}
