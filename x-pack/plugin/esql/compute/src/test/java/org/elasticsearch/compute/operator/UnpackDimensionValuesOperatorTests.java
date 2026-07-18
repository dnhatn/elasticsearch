/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Releasables;

import static org.hamcrest.Matchers.equalTo;

public class UnpackDimensionValuesOperatorTests extends ComputeTestCase {

    public void testUnpackRoundTrip() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        String[] keywords = { "a", "b", null, "d" };
        long[] numbers = { 1L, 2L, 0L, 4L };
        int positions = keywords.length;

        BytesRefBlock keyword;
        LongBlock number;
        try (var kb = blockFactory.newBytesRefBlockBuilder(positions); var nb = blockFactory.newLongBlockBuilder(positions)) {
            for (int p = 0; p < positions; p++) {
                if (keywords[p] == null) {
                    kb.appendNull();
                    nb.appendNull();
                } else {
                    kb.appendBytesRef(new BytesRef(keywords[p]));
                    nb.appendLong(numbers[p]);
                }
            }
            keyword = kb.build();
            number = nb.build();
        }

        ElementType[] types = { ElementType.BYTES_REF, ElementType.LONG };
        BytesRefBlock packed = DimensionPacker.packMultiColumns(driverContext, new Block[] { keyword, number });

        Page page = new Page(packed); // channel 0 = packed
        BytesRefBlock outKeyword;
        LongBlock outNumber;
        try (var op = new UnpackDimensionValuesOperator(driverContext, 0, types)) {
            op.addInput(page);
            Page out = op.getOutput();
            assertThat(out.getBlockCount(), equalTo(3)); // packed + 2 unpacked
            outKeyword = out.getBlock(1);
            outNumber = out.getBlock(2);
            outKeyword.incRef();
            outNumber.incRef();
            out.releaseBlocks();
        }

        try {
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < positions; p++) {
                if (keywords[p] == null) {
                    assertTrue(outKeyword.isNull(p));
                    assertTrue(outNumber.isNull(p));
                } else {
                    assertThat(outKeyword.getBytesRef(outKeyword.getFirstValueIndex(p), scratch), equalTo(new BytesRef(keywords[p])));
                    assertThat(outNumber.getLong(outNumber.getFirstValueIndex(p)), equalTo(numbers[p]));
                }
            }
        } finally {
            Releasables.close(keyword, number, outKeyword, outNumber);
        }
    }
}
