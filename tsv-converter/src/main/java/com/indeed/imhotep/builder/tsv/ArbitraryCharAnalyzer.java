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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

import java.io.IOException;
import java.io.Reader;

/**
 * @author kbinswanger
 */
public class ArbitraryCharAnalyzer extends Analyzer {
    private final char delimeter;

    public ArbitraryCharAnalyzer(final char delimeter) {
        this.delimeter = delimeter;
    }

    public TokenStream tokenStream(final String fieldName, final Reader reader) {
        return new ArbitraryCharTokenizer(reader, delimeter);
    }

    // Copied from {@link WhitespaceAnalyzer}
    public TokenStream reusableTokenStream(final String fieldName, final Reader reader) throws IOException {
        Tokenizer tokenizer = (Tokenizer) getPreviousTokenStream();
        if (tokenizer == null) {
            tokenizer = new ArbitraryCharTokenizer(reader, delimeter);
            setPreviousTokenStream(tokenizer);
        } else {
            tokenizer.reset(reader);
        }
        return tokenizer;
    }

    private class ArbitraryCharTokenizer extends CharTokenizer {
        private final char delimeter;

        private ArbitraryCharTokenizer(final Reader in, final char delimeter) {
            super(in);
            this.delimeter = delimeter;
        }

        @Override
        protected boolean isTokenChar(final char c) {
            return c != delimeter;
        }
    }
}
