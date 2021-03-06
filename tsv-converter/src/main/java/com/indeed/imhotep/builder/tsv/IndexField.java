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

import javax.annotation.Nullable;

/**
 * @author vladimir
 */

public class IndexField {
    private final String name;
    private boolean tokenized;
    private boolean bigram;
    private boolean idxFullField;
    private FieldType fieldType;
    private boolean fieldTypeGuaranteed;
    private final String nameTokenized;
    private final String nameBigram;
    private int illegalIntValues = 0;
    private final Analyzer analyzer;


    public IndexField(final String name, final boolean tokenized, final boolean bigram, final boolean idxFullField,
                      @Nullable final Character delimeter, FieldType fieldType) {
        this.name = name;
        nameTokenized = name + "tok";
        nameBigram = name + "bigram";
        this.tokenized = tokenized;
        this.bigram = bigram;
        this.idxFullField = idxFullField;
        this.fieldType = fieldType;
        this.fieldTypeGuaranteed = fieldType != null;

        if(delimeter != null) {
            analyzer = new ArbitraryCharAnalyzer(delimeter);
        } else {
            analyzer = null;
        }
    }

    public String getName() {
        return name;
    }

    public String getNameTokenized() {
        return nameTokenized;
    }

    public String getNameBigram() {
        return nameBigram;
    }

    public boolean isTokenized() {
        return tokenized;
    }

    public boolean isBigram() {
        return bigram;
    }
    
    public boolean isIdxFullField() {
        return idxFullField;
    }

    public Analyzer getAnalyzer() {
        return analyzer;
    }

    public boolean isIntField() {
        return fieldType == FieldType.INT;
    }

    public void setIntField(boolean intField) {
        fieldType = intField ? FieldType.INT : FieldType.STRING;
    }

    public boolean isFieldTypeGuaranteed() {
        return fieldTypeGuaranteed;
    }

    public void incrementIllegalIntValue() {
        illegalIntValues++;
    }

    public int getIllegalIntValues() {
        return illegalIntValues;
    }
}
