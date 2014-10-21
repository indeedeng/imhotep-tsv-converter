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

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVReader;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

/**
 * @author vladimir
 */

public class CSVInputReader implements InputReader {
    private static final Logger log = Logger.getLogger(CSVInputReader.class);

    private final CSVReader reader;

    public CSVInputReader(Reader fileReader, char separator) {
        final boolean ignoreLeadingWhitespace = false;
        reader = new CSVReader(fileReader, separator, CSVParser.DEFAULT_QUOTE_CHARACTER,
                CSVParser.DEFAULT_ESCAPE_CHARACTER, CSVReader.DEFAULT_SKIP_LINES, CSVParser.DEFAULT_STRICT_QUOTES, ignoreLeadingWhitespace);
    }

    public CSVInputReader(Reader fileReader) {
        this(fileReader, ',');
    }

    public Iterator<String[]> iterator() {
        return new AbstractIterator<String[]>() {
            protected String[] computeNext() {
                final String[] next;
                try {
                    next = reader.readNext();
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
                if(next == null) {
                    Closeables2.closeQuietly(reader, log);
                    return endOfData();
                }

                return next;
            }
        };
    }

    @Override
    public void close() {
        Closeables2.closeQuietly(reader, log);
    }
}
