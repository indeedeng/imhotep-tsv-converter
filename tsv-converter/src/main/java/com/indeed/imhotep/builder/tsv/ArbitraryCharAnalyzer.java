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
        protected boolean isTokenChar(char c) {
            return c == delimeter;
        }
    }
}
