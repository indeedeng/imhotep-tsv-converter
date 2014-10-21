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
