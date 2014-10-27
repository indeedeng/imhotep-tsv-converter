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
 package com.indeed.imhotep.builder.tsv;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.indeed.util.core.io.Closeables2;
import com.indeed.imhotep.builder.tsv.input.CSVInputReader;
import com.indeed.imhotep.builder.tsv.input.InputReader;
import com.indeed.imhotep.builder.tsv.input.TSVInputReaderNoEscaping;
import com.indeed.imhotep.index.builder.util.EasyIndexBuilder;
import com.indeed.imhotep.index.builder.util.IndexWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.zip.GZIPInputStream;

/**
 * @author vladimir
 */

@SuppressWarnings("UnusedDeclaration")
public class EasyIndexBuilderFromTSV extends EasyIndexBuilder {
    static {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT-6"));
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
    }
    private static final Logger log = Logger.getLogger(EasyIndexBuilderFromTSV.class);
    protected int timeFieldIndex = -1;
    protected long startTimestampMS;
    protected long endTimestampMS;
    protected IndexField[] indexFields;
    protected boolean PRODUCE_FLAMDEX = true;
    int rowCount = 0;

    @Override
    protected void setup() {
        super.setup();
//        if(getInputFilePath().toString().startsWith("hdfs:")) {
//            // need to login to kerberos
//            try {
//                KerberosUtils.loginFromKeytab(null, null);
//            } catch (IOException e) {
//                log.error("Failed to log in to Kerberos", e);
//            }
//        }
    }

    protected InputReader getInputReader() {
        final Path inputFile = getInputFilePath();
        final BufferedReader fileReader = getInputFileReader(inputFile);
        final String fileName = inputFile.getName();
        if(fileName.endsWith(".csv") || fileName.endsWith(".csv.gz")) {
            return new CSVInputReader(fileReader);
        } else {
            // Should we also using the escaped TSV in some situations? What would be the trigger?
            return new TSVInputReaderNoEscaping(fileReader);
        }
    }

    @Override
    protected void init() {
        super.init();

        if(PRODUCE_FLAMDEX) {
            final IndexWriter indexWriter = new FlamdexIndexWriter(indexDir);
            setIndexWriter(indexWriter);
        }

        startTimestampMS = options.start;
        endTimestampMS = options.end;

        final InputReader reader = getInputReader();
        try {
            final Iterator<String[]> iterator = reader.iterator();
            inferFieldsFromHeader(iterator);
            if(PRODUCE_FLAMDEX) {
                detectIntFields(iterator);
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        } finally {
            Closeables2.closeQuietly(reader, log);
        }
    }

    private void detectIntFields(Iterator<String[]> iterator) throws IOException {
        final int[] intValCount = new int[indexFields.length];
        final boolean[] isInt = new boolean[indexFields.length];
        Arrays.fill(isInt, true);
        if(!iterator.hasNext()) {
            throw new RuntimeException("No data is available in the input file. At least one line of data past the header is required");
        }
        log.info("Scanning the file to detect int fields");
        while(iterator.hasNext()) {
            final String[] values = iterator.next();
            final int valueCount = Math.min(values.length, indexFields.length);
            rowCount++;
            for(int i = 0; i < valueCount; i++) {
                if(!isInt[i]) {
                    continue;   // we already know this is not an integer
                }

                if(Longs.tryParse(values[i]) != null) {
                    intValCount[i]++;
                }

                if(rowCount > 10000 && intValCount[i] < rowCount / 10 * 9) {
                    isInt[i] = false;
                }
            }
        }
        int intFieldCount = 0;
        List<String> intFields = Lists.newArrayList();
        for(int i = 0; i < indexFields.length; i++) {
            boolean isIntField = isInt[i] && intValCount[i] > rowCount / 10 * 9; // over 90% fit
            if(isIntField) {
                intFields.add(indexFields[i].getName());
            }
            indexFields[i].setIntField(isIntField);
        }
        Collections.sort(intFields);
        log.info("Int fields detected: " + Joiner.on(",").join(intFields));
    }

    private void inferFieldsFromHeader(Iterator<String[]> iterator) throws IOException {
        if(!iterator.hasNext()) {
            throw new RuntimeException("The provided file didn't have a header with field names in the first line");
        }

        final String[] fieldHeaders = iterator.next();
        indexFields = new IndexField[fieldHeaders.length];
        for(int i = 0; i < fieldHeaders.length; i++) {
            String field = fieldHeaders[i];
            boolean tokenized = false;
            boolean bigram = false;
            boolean idxFullField = true;
            if(field.endsWith("**")) {
                bigram = true;
                tokenized = true;
                idxFullField = true;
                field = field.substring(0, field.length()-2);
            } else if(field.endsWith("*")) {
                bigram = false;
                tokenized = true;
                idxFullField = true;
                field = field.substring(0, field.length()-1);
            } else if(field.endsWith("+")) {
                bigram = false;
                tokenized = true;
                field = field.substring(0, field.length()-1);
                idxFullField = false;
            }

            validateFieldName(field);

            final IndexField indexField = new IndexField(field, tokenized, bigram, idxFullField);
            indexFields[i] = indexField;
            if("time".equals(field) || "unixtime".equals(field)) {
                timeFieldIndex = i;
            }
        }
    }

    private BufferedReader getInputFileReader(Path inputFile) {
        try {
            final FileSystem hdfs = getHDFS(inputFile);
            final Path qualifiedInputFile = inputFile.makeQualified(hdfs);
            if(!hdfs.exists(inputFile)) {
                throw new RuntimeException("The provided input file doesn't exist " + qualifiedInputFile +
                        "\nFor hdfs files use 'hdfs:' prefix like hdfs:/tmp/file.tsv");
            }
            log.info("Reading TSV data from " + qualifiedInputFile);
            InputStream inputStream = hdfs.open(inputFile);
            if(inputFile.getName().endsWith(".gz")) {
                inputStream = new GZIPInputStream(inputStream);
            }
            return new BufferedReader(new InputStreamReader(inputStream, Charsets.UTF_8));
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private void validateFieldName(String field) {
        final String validationRegex = "[A-Za-z_][A-Za-z_0-9]*";
        if(!field.matches(validationRegex)) {
            throw new RuntimeException("Please make field name " + field + " conform to regex: " + validationRegex);
        }
    }

    @Override
    protected void loop() {
        final InputReader reader = getInputReader();
        try {
            Iterator<String[]> iterator = reader.iterator();
            iterator.next();  // skip header
            while(iterator.hasNext()) {
                final String[] values = iterator.next();
                final int valueCount = Math.min(values.length, indexFields.length);
                if(valueCount != indexFields.length) {
                    // inconsistent number of columns detected
                    // TODO: error? log?
                }
                long docTimestamp = startTimestampMS;   // default in case we don't have a time column
                for(int i = 0; i < valueCount; i++) {
                    final String value = values[i];
                    final IndexField field = indexFields[i];
                    if(i == timeFieldIndex) {
                        long timestamp;
                        try {
                            timestamp = Long.parseLong(value);
                            if(timestamp < Integer.MAX_VALUE) {
                                timestamp *= 1000;  // assume it's in seconds and convert to milliseconds
                            }
                        } catch (NumberFormatException e) {
                            // TODO
//                            log.warn("Illegal timestamp: " + value);
                            continue;
                        }
                        if(timestamp < startTimestampMS || timestamp > endTimestampMS) {  // should this be inclusive of endTS?
                            log.warn("Timestamp outside range: " + timestamp + ". Should be between: " + startTimestampMS + " and " + endTimestampMS);
                            continue;
                        }
                        docTimestamp = timestamp;
                    } else {
                        if(field.isIntField()) {
                            final Long intValue = Longs.tryParse(value);
                            if(intValue != null) {
                                addTerm(field.getName(), intValue);
                            } else {
                                addTerm(field.getName(), 0);
                                field.incrementIllegalIntValue();
                            }
                        } else {    // string term
                            if(field.isIdxFullField()) {
                                addTerm(field.getName(), value, false);
                            }
                            if(field.isTokenized()) {
                                /* Use the tokenized field name only 
                                 * if the full name has already been used 
                                 */
                                String fn = field.isIdxFullField() ? field.getNameTokenized()
                                                                   : field.getName();
                                addTerm(fn, value, true);
                            }
                            if(field.isBigram()) {
                                addBigramTerm(field.getNameBigram(), value);
                            }
                        }
                    }
                }

                saveDocument(docTimestamp);
            }
            for(IndexField field : indexFields) {
                int badIntVals = field.getIllegalIntValues();
                if(badIntVals > 0) {
                    log.warn("Column " + field.getName() + " had " + badIntVals + " (" + badIntVals * 100 / rowCount + "%) illegal int values");
                }
            }
        } finally {
            Closeables2.closeQuietly(reader, log);
        }
    }

    public FileSystem getHDFS(Path inputFilePath) {
        try {
            return inputFilePath.getFileSystem(new org.apache.hadoop.conf.Configuration());
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    protected Path getInputFilePath() {
        String inputFilePath = options.extra;
        if(Strings.isNullOrEmpty(inputFilePath)) {
            throw new RuntimeException("Expecting extra arg to be the HDFS path of the TSV input file.");
        }
        if (!inputFilePath.startsWith("hdfs:") && !inputFilePath.startsWith("s3n:")
                && !inputFilePath.startsWith("file:")) {
            inputFilePath = "file:" + inputFilePath;
        }
        return new Path(inputFilePath);
    }
}
