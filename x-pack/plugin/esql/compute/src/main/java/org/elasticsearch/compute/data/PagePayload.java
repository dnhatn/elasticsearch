/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

/**
 * The bytes a page's values spell, as against the memory its blocks hold. It is the volume a reader produced and a
 * consumer walks, so it is what a rate over a page is taken against.
 *
 * <p>A value of a fixed width is counted by its width, and one of bytes by its own length. Nothing is copied, and
 * nothing is counted twice: a column that holds the sum of its values is asked for it, one read against a
 * dictionary measures each term once, and only a shape that answers for no more than one value at a time is
 * walked a value at a time.
 */
public final class PagePayload {

    private PagePayload() {}

    /** The bytes the values of {@code page} spell. */
    public static long of(Page page) {
        long bytes = 0;
        for (int channel = 0; channel < page.getBlockCount(); channel++) {
            final Block block = page.getBlock(channel);
            bytes += of(block);
        }
        return bytes;
    }

    /** The bytes the values of {@code block} spell, or 0 where it holds none. */
    public static long of(Block block) {
        final int positions = block.getPositionCount();
        if (positions == 0 || block.areAllValuesNull()) {
            return 0;
        }
        if (block instanceof BytesRefBlock bytesRefs) {
            final long total = totalOf(bytesRefs);
            return total >= 0 ? total : walk(bytesRefs);
        }
        final int width = widthOf(block.elementType());
        return width == 0 ? 0 : (long) block.getTotalValueCount() * width;
    }

    /**
     * The bytes a block of them holds where its shape answers for all of its values at once, or {@code -1} where
     * only walking them answers. A column arrives in one of a few shapes, and most of them already hold the sum
     * or hold each value once however many positions name it.
     */
    private static long totalOf(BytesRefBlock block) {
        if (block instanceof BytesRefArrayBlock arrayBlock) {
            return arrayBlock.totalValueBytes();
        }
        final OrdinalBytesRefBlock ordinals = block.asOrdinals();
        if (ordinals != null && ordinals.getDictionaryVector().getPositionCount() <= block.getTotalValueCount()) {
            // A dictionary holding more terms than the block holds values is read for fewer of them than it holds,
            // so measuring all of it would cost more than reading the values it is asked for.
            return ordinals.totalValueBytes();
        }
        final BytesRefVector vector = block.asVector();
        if (vector instanceof BytesRefArrayVector arrayVector) {
            return arrayVector.totalValueBytes();
        }
        if (vector != null && vector.isConstant()) {
            // One value, named by every position: its length is what each of them weighs.
            return (long) vector.getPositionCount() * vector.getBytesRef(0, new BytesRef()).length;
        }
        return -1;
    }

    /**
     * The bytes a column read against a dictionary holds, taken as the length of the term each position names.
     * The terms are measured once each, which is what reading them as ordinals saved in the first place.
     */
    static long ordinalsOf(OrdinalBytesRefBlock block) {
        final BytesRefVector dictionary = block.getDictionaryVector();
        assert dictionary.getPositionCount() <= block.getTotalValueCount() : "measuring the dictionary costs more";
        final BytesRef scratch = new BytesRef();
        final int[] lengths = new int[dictionary.getPositionCount()];
        for (int term = 0; term < lengths.length; term++) {
            lengths[term] = dictionary.getBytesRef(term, scratch).length;
        }
        final IntBlock ordinals = block.getOrdinalsBlock();
        final int positions = ordinals.getPositionCount();
        long bytes = 0;
        for (int position = 0; position < positions; position++) {
            final int count = ordinals.getValueCount(position);
            final int first = ordinals.getFirstValueIndex(position);
            for (int value = 0; value < count; value++) {
                bytes += lengths[ordinals.getInt(first + value)];
            }
        }
        return bytes;
    }

    /** The bytes a block of them holds, read as the length of each value it holds. */
    private static long walk(BytesRefBlock block) {
        final int positions = block.getPositionCount();
        final BytesRef scratch = new BytesRef();
        long bytes = 0;
        for (int position = 0; position < positions; position++) {
            final int count = block.getValueCount(position);
            final int first = block.getFirstValueIndex(position);
            for (int value = 0; value < count; value++) {
                bytes += block.getBytesRef(first + value, scratch).length;
            }
        }
        return bytes;
    }

    /** The bytes one value of {@code type} spells, or 0 where it has no width of its own. */
    private static int widthOf(ElementType type) {
        return switch (type) {
            case BOOLEAN -> Byte.BYTES;
            case INT, FLOAT -> Integer.BYTES;
            case LONG, DOUBLE -> Long.BYTES;
            default -> 0;
        };
    }
}
