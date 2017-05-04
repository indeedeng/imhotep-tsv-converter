/*
 * Copyright (C) 2014 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.imhotep.index.builder.util;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * @author mmorrison
 * @author jchien
 */
public abstract class EasyIndexBuilder implements Bootable {
    private static final Logger logger = Logger.getLogger(EasyIndexBuilder.class);
    public static final TimeZone tz = TimeZone.getTimeZone("GMT-6");
    static {
        TimeZone.setDefault(tz);
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
    }

    // The name for this index, as reflected in the folder structure, as well as the ramses GUI
    // If null, primaryLogType will be used as name.
    protected String name = null;

    // A list of all the fields to be indexed.
    // See EasyIndexField for the syntax of a field.
    protected EasyIndexField[] fields;

    /**
     * A list of subindexes. This list is written out to the subindexes.desc meta-file.
     */
    protected String[] subIndexes = null;

    protected EasyIndexBuilderOptions options = new EasyIndexBuilderOptions();
    protected String indexDir;
    protected IndexWriter indexWriter;

    /**
     * In case your builder requires additional command line parameters, you can add your own SmartArgs by
     * calling .addExtension on the parameter to this function. Check EasyIndexBuilderFromLogs for an example.
     */
    protected void extendOptions(SmartArgs options) { }

    // this is where your subclass can set all these cool options
    // do not do any substantial work in this method. Database connections
    // and other handles should be opened in the init() method below.
    // setup() is purely for settings the flags which configure this class.
    protected void setup() {
    }
    
    public void runSetup() {
        this.setup();
    }

    // Setup database connections and other substantial initialization work here.
    protected void init() {
    }
    
    // Setup logging
    protected void loggingInit() {
    }

    // This is where the index is created. Within this function, you should call addDocument() for each
    // document to be added to the index.
    protected void loop() {
    }

    protected void setIndexWriter(IndexWriter indexWriter) {
        this.indexWriter = indexWriter;
    }

    // any class that inherits this can be run directly from Bootstrap, cool!
    final public int run(String[] args) throws IOException {
        EasyIndexBuilderOptions options = new EasyIndexBuilderOptions();
        extendOptions(options);
        options.parse(args);

        setOptions(options);
        return build();
    }


    final public void setOptions(EasyIndexBuilderOptions options) {
        this.options = options;
    }

    final public int build() throws IOException {
        setup(); // finish the setup of our preference fields

        indexDir = getIndexDir(options);

        logger.info("start=" + options.start);
        logger.info("finish=" + options.end);
        logger.info("index=" + indexDir);

        init(); // do the heavy lifting to initialize the builder

        logger.info("Creating index...");
        {
            File dir = new File(indexDir);
            if(dir.exists()) {
//                logger.info("The index for this time period already exists.");
                if(options.skip) {
                    return 0;
                } else if(options.overwrite) {
                    FileUtils.deleteDirectory(dir);
                } else {
                    logger.error("This error is fatal");
                    logger.error("Use --overwrite to destroy the old index");
                    logger.error("Use --skip to abort and exit without an error");
                    return 1;
                }
            }
            if (!dir.mkdirs()) {
                throw new RuntimeException("Could not create index directory: " + indexDir);
            }
        }
        if(indexWriter == null) {
            throw new RuntimeException("IndexWriter not set.");
        }

        logger.info("Calling builder loop...");
        loop();

        int count = indexWriter.getNumDocs();
        logger.info("Wrote " + count + " Documents");

        logger.info("finalizing index");
        indexWriter.completeIndex();

        if(count <= 0) {
            logger.error("No documents were found for this index, and this builder is defined to fail fatally on empty indexes.");
            FileUtils.deleteDirectory(new File(indexDir));
            return -1;
        }

        logger.info("done");
        return 0;
    }

    public static String getIndexDir(EasyIndexBuilderOptions options) {
        final DateFormat fileDateFormatter = new SimpleDateFormat("yyyyMMdd.HH");
        String shardStart = fileDateFormatter.format(options.start);
        String shardEnd = fileDateFormatter.format(options.end);
        String shardName = "index" + shardStart + "-" + shardEnd;
        return buildPath(options.baseDir, shardName);
    }

    /**
     * Allows manual document construction as an alternative to addDocument()
     */
    final protected void addTerm(String fieldName, String term, boolean tokenized) {
        indexWriter.addTerm(fieldName, term, tokenized);
    }

    final protected void addTerm(String fieldName, String term, Analyzer analyzer) {
        indexWriter.addTerm(fieldName, term, analyzer);
    }

    final protected void addBigramTerm(String fieldName, String term) {
        indexWriter.addBigramTerm(fieldName, term);
    }

    /**
     * Allows manual document construction as an alternative to addDocument()
     */
    final protected void addTerm(String fieldName, long intTerm) {
        indexWriter.addTerm(fieldName, intTerm);
    }

    /**
     * Allows manual document construction as an alternative to addDocument()
     */
    final protected void saveDocument(long docTimestamp) {
        indexWriter.addTerm("allbit", "1", false);

        indexWriter.saveDocument(docTimestamp);
        int numDocs = indexWriter.getNumDocs();
        if (numDocs % 100000 == 0) {
            logger.info("added " + numDocs + " documents, docTimestamp is " + new Timestamp(docTimestamp));
        }
    }

    final protected void addDocument(EasyIndexDocument doc) {
        for(EasyIndexField field : fields) {
            String value = doc.get(field.indexKey);
            if(value == null) continue;

            if(field.isDimension && field.dimensionValueParser == null) {
                // force it to an int if it's not already!
                // Only if dimensionValueParser is null
                long ivalue;
                try {
                    ivalue = Long.parseLong(value);
                } catch (NumberFormatException e) {
                    ivalue = 0;
                }

                addTerm(field.indexKey, ivalue);
            } else {
                addTerm(field.indexKey, value, field.tokenized);
            }
        }

        saveDocument(doc.timestamp);
    }

    private static final String buildPath(String ... parts) {
        if (parts.length == 0) return null;
        if (parts.length == 1) return parts[0];
        File temp = new File(parts[0], parts[1]);
        for (int i = 2; i < parts.length; i++) {
            temp = new File(temp, parts[i]);
        }
        return temp.getPath();
    }

    public String getName() {
        return name;
    }

    public EasyIndexField[] getFields() {
        return fields;
    }

    public String[] getSubIndexes() {
        return subIndexes;
    }

    public EasyIndexBuilderOptions getOptions() {
        return options;
    }

    public String getIndexDir() {
        return indexDir;
    }

    public IndexWriter getIndexWriter() {
        return indexWriter;
    }

}
