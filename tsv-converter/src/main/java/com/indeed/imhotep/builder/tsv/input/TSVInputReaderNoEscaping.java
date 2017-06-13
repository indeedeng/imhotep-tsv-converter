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

import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Iterator;

/**
 * @author vladimir
 */

public class TSVInputReaderNoEscaping implements InputReader {
    private static final Logger log = Logger.getLogger(TSVInputReaderNoEscaping.class);

    private final BufferedReader fileReader;

    public TSVInputReaderNoEscaping(BufferedReader fileReader) {
        this.fileReader = fileReader;
    }

    @Override
    public Iterator<String[]> iterator() {
        return new AbstractIterator<String[]>() {
            protected String[] computeNext() {
                final String nextLine;
                try {
                     nextLine = fileReader.readLine();
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
                if(nextLine == null) {
                    Closeables2.closeQuietly(fileReader, log);
                    return endOfData();
                }

                // limit=-1 preserves the empty value after the trailing \t
                return nextLine.split("\t", -1);
            }
        };
    }

    @Override
    public void close() {
        Closeables2.closeQuietly(fileReader, log);
    }
}
