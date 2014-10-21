package com.indeed.imhotep.index.builder.util;

import javax.annotation.Nonnull;

/**
 * Abstracts creating an inverted index e.g. in Lucene or Flamdex format.
 * @author vladimir
 */
public interface IndexWriter {

    public void addTerm(@Nonnull String fieldName, @Nonnull String stringTerm, boolean tokenized);
    public void addTerm(@Nonnull String fieldName, long numTerm);
    public void addBigramTerm(@Nonnull String fieldName, @Nonnull String stringTerm);

    /**
     * Causes the current document to be stored in the index.
     * @param docTimestamp Timestamp value to be assigned to the document. Should fall within the shard time range.
     */
    public void saveDocument(long docTimestamp);
    /**
     * Finalizes the index and returns the number of written docs.
     */
    public int completeIndex();
    /**
     * Returns the number of documents written so far.
     */
    public int getNumDocs();
}
