/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.postings;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.tests.index.BasePostingsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class ES814InlinePostingsFormatTests extends BasePostingsFormatTestCase {

    private final Codec codec = TestUtil.alwaysPostingsFormat(new ES814InlinePostingsFormat());

    @Override
    protected Codec getCodec() {
        return codec;
    }

}
