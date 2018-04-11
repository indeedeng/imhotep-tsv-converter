/*
 * Copyright (C) 2018 Indeed Inc.
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

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.indeed.flamdex.simple.SimpleFlamdexDocWriter;
import com.indeed.flamdex.writer.FlamdexDocWriter;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.imhotep.index.builder.util.IndexWriter;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceAnalyzer;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author vladimir
 */

public class FlamdexIndexWriter implements IndexWriter {
    private static final Logger log = Logger.getLogger(FlamdexIndexWriter.class);
    private static final String TIME_FIELD_NAME = "unixtime";

    private final FlamdexDocWriter indexWriter;
    private final Analyzer analyzer = new WhitespaceAnalyzer();
    private FlamdexDocument flamdexDocument = new FlamdexDocument();
    private int numDocs = 0;

    public FlamdexIndexWriter(@Nonnull String outputDirectory) {

        log.debug("Creating index in " + outputDirectory);
        {
            final File dir = new File(outputDirectory);
            if (!dir.exists() && !dir.mkdirs()) {
                throw new RuntimeException("Could not create index directory: " + outputDirectory);
            }
        }
        try {
            indexWriter = new SimpleFlamdexDocWriter(outputDirectory, new SimpleFlamdexDocWriter.Config().setDocBufferSize(10000));
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void addBigramTerm(@Nonnull String fieldName, @Nonnull String stringTerm) {
        final List<String> bigrams = getBigrams(stringTerm);
        flamdexDocument.addStringTerms(fieldName, bigrams);
    }

    private List<String> getBigrams(String stringTerm) {
        final List<String> words = tokenize(stringTerm, analyzer);
        if(words.size() < 2) {
            return Collections.emptyList();
        }
        final List<String> bigrams = Lists.newArrayListWithCapacity(words.size() - 1);
        for(int i = 0; i < words.size() - 1; i++) {
            bigrams.add(words.get(i) + " " + words.get(i+1));
        }
        return bigrams;
    }

    @Override
    public void addTerm(@Nonnull String fieldName, @Nonnull String stringTerm, boolean tokenized) {
        if(!tokenized) {
            flamdexDocument.addStringTerm(fieldName, stringTerm);
        } else {
            flamdexDocument.addStringTerms(fieldName, tokenize(stringTerm, analyzer));
        }
    }

    @Override
    public void addTerm(@Nonnull String fieldName, @Nonnull String stringTerm, @Nonnull Analyzer overrideAnalyzer) {
        flamdexDocument.addStringTerms(fieldName, tokenize(stringTerm, overrideAnalyzer));
    }

    @Override
    public void addTerm(@Nonnull String fieldName, long numTerm) {
        flamdexDocument.addIntTerm(fieldName, numTerm);
    }

    @Override
    public void saveDocument(long docTimestamp) {
        flamdexDocument.addIntTerm(TIME_FIELD_NAME, docTimestamp / 1000);
        try {
            indexWriter.addDocument(flamdexDocument);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        numDocs++;
        // clear instead of recreate for garbage reduction
        flamdexDocument.getIntFields().clear();
        flamdexDocument.getStringFields().clear();
    }

    @Override
    public int completeIndex() {
        try {
            indexWriter.close();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return numDocs;
    }

    @Override
    public int getNumDocs() {
        return numDocs;
    }

    // copy-paste from com.indeed.imhotep.builders.SearchParserUtils.tokenize()
    private static List<String> tokenize(String s, Analyzer analyzer) {
        final TokenStream tokenStream = analyzer.tokenStream("", new StringReader(s));
        final Token token = new Token();
        final List<String> ret = new ArrayList<String>();
        try {
            while (tokenStream.next(token) != null) {
                ret.add(token.term());
            }
        } catch (IOException e) {
            throw new RuntimeException(e); // should never happen since using StringReader
        }
        return ret;
    }
}
