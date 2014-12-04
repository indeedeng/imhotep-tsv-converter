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
 package com.indeed.imhotep.builder.tsv.input;

import com.google.common.collect.AbstractIterator;
import com.indeed.util.core.io.Closeables2;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

/**
 * @author vladimir
 */

public class CSVInputReader implements InputReader {
    private static final Logger log = Logger.getLogger(CSVInputReader.class);

    private final CSVParser parser;
    private final Iterator<CSVRecord> iter;

    public CSVInputReader(Reader fileReader, char separator) throws IOException {
        final boolean ignoreLeadingWhitespace = false;
        final CSVFormat csvFormat;

        csvFormat = CSVFormat.DEFAULT
                .withAllowMissingColumnNames(false)
                .withRecordSeparator(separator)
                .withRecordSeparator('\n')
                .withEscape('\\')
                .withIgnoreSurroundingSpaces(ignoreLeadingWhitespace)
                .withIgnoreEmptyLines(false);
        this.parser = csvFormat.parse(fileReader);
        this.iter = this.parser.iterator();
    }

    public CSVInputReader(Reader fileReader) throws IOException {
        this(fileReader, ',');
    }

    public final Iterator<String[]> iterator() {
        return new AbstractIterator<String[]>() {
            protected String[] computeNext() {

                if (! iter.hasNext()) {
                    Closeables2.closeQuietly(parser, log);
                    return endOfData();
                }
                
                final CSVRecord rec = iter.next();
                final String[] next = new String[rec.size()];
                for (int i = 0; i < rec.size(); i++) {
                    next[i] = rec.get(i);
                }
                return next;
            }
        };
    }

    @Override
    public final void close() {
        Closeables2.closeQuietly(parser, log);
    }
}
