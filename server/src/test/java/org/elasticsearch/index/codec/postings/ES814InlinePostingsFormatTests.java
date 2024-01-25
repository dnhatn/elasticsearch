/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.postings;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.tests.index.BasePostingsFormatTestCase;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ES814InlinePostingsFormatTests extends BasePostingsFormatTestCase {

    private final Codec codec = TestUtil.alwaysPostingsFormat(new ES814InlinePostingsFormat());

    @Override
    protected Codec getCodec() {
        return codec;
    }

    public void testNextAfterSeekExact() throws IOException {
        BaseDirectoryWrapper dir = newDirectory();
        IndexWriterConfig config = new IndexWriterConfig();
        config.setCodec(codec);
        IndexWriter writer = new IndexWriter(dir, config);
        for (int i = 10; i < 100; i++) {
            writer.addDocument(List.of(newStringField("f", String.format("v%d", i), Field.Store.NO)));
        }
        DirectoryReader reader = DirectoryReader.open(writer);
        LeafReader leaf = reader.leaves().get(0).reader();
        Terms terms = leaf.terms("f");

        TermsEnum termsEnum = terms.iterator();
        termsEnum.seekExact(new BytesRef("v25"));
        TermState s25 = termsEnum.termState();
        termsEnum.seekExact(new BytesRef("v45"));
        TermState s45 = termsEnum.termState();

        for (int i = 5; i > 0; i--) {
            termsEnum.seekExact(new BytesRef("v25"), s25);
            if (random().nextBoolean()) {
                s25 = termsEnum.termState();
            }
            assertThat(termsEnum.next(), equalTo(new BytesRef("v26")));
            assertThat(termsEnum.next(), equalTo(new BytesRef("v27")));
            termsEnum.seekExact(new BytesRef("v45"), s45);
            if (random().nextBoolean()) {
                s45 = termsEnum.termState();
            }
            assertThat(termsEnum.next(), equalTo(new BytesRef("v46")));
            assertThat(termsEnum.next(), equalTo(new BytesRef("v47")));
        }
        IOUtils.close(reader, writer, dir);
    }

    @Override
    public void testDocsAndFreqsAndPositionsAndPayloads() throws Exception {
        super.testDocsAndFreqsAndPositionsAndPayloads();
    }
}
