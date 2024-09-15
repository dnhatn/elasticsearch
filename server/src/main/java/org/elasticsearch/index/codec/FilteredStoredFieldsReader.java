/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.util.Bits;

import java.io.IOException;

public class FilteredStoredFieldsReader extends StoredFieldsReader {
    public final StoredFieldsReader in;
    public final Bits unchangedDocs;

    public FilteredStoredFieldsReader(StoredFieldsReader fieldsReader, Bits unchangedDocs) {
        this.in = fieldsReader;
        this.unchangedDocs = unchangedDocs;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public void document(int docID, StoredFieldVisitor visitor) throws IOException {
        in.document(docID, visitor);
    }

    @Override
    public StoredFieldsReader getMergeInstance() {
        return new FilteredStoredFieldsReader(in.getMergeInstance(), unchangedDocs);
    }

    @Override
    public StoredFieldsReader clone() {
        return new FilteredStoredFieldsReader(in.clone(), unchangedDocs);
    }

    @Override
    public void checkIntegrity() throws IOException {
        in.checkIntegrity();
    }
}
