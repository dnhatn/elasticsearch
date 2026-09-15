/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.MixHash64;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Hand-written {@link ExpressionEvaluator} for {@code REPLACE(str, regex, newStr)} when both
 * {@code regex} and {@code newStr} are foldable.
 * <p>
 * When the input column is dictionary-encoded ({@link OrdinalBytesRefBlock}) and single-valued and dense,
 * REPLACE is applied once per dictionary entry and the result is emitted as a fresh
 * {@link OrdinalBytesRefBlock} that re-uses the original ordinals. For a column with N positions backed
 * by a dictionary of size D, this reduces regex work from N calls down to D — typically a 10–50x
 * reduction on Lucene keyword columns where doc-value ordinals naturally deduplicate.
 * <p>
 * The fast-path is correctness-equivalent to the per-row path because:
 * <ul>
 *   <li>REPLACE is a pure function of its inputs, so {@code f(input)} is the same regardless of how many
 *       rows reference the same dictionary entry.</li>
 *   <li>Dictionary entries are never null (the {@link BytesRefVector} contract), so the row-level
 *       null-out logic from the per-row path doesn't apply here — nulls are already represented in
 *       the ordinals {@link IntBlock} and carried over unchanged.</li>
 *   <li>If {@link Replace#process} throws {@link IllegalArgumentException} (result-too-large) for any
 *       dictionary entry, we abandon the dictionary path for this page and fall back to per-row
 *       evaluation. The fallback emits warnings exactly as the existing per-row path does.</li>
 * </ul>
 * <p>
 * The dictionary path is gated by {@link OrdinalBytesRefBlock#isDense()} and a no-multi-value check;
 * when those don't hold, evaluation goes through the same per-row loop as
 * {@code ReplaceConstantEvaluator}.
 * <p>
 * The successful dictionary path returns an {@link OrdinalBytesRefBlock}; the per-row fallback returns
 * a materialized {@code BytesRefBlock} produced by a builder. Logical equality (values, nulls) is
 * identical between the two — see {@code BytesRefBlock.equals(BytesRefBlock, BytesRefBlock)} — but the
 * underlying block type may differ. Callers that pattern-match on block identity must not rely on
 * either shape.
 */
final class ReplaceConstantOrdinalEvaluator implements ExpressionEvaluator {
    private static final Logger logger = LogManager.getLogger(ReplaceConstantOrdinalEvaluator.class);
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ReplaceConstantOrdinalEvaluator.class);

    private final Source source;
    private final ExpressionEvaluator str;
    private final Pattern regex;
    private final byte[] literalPrefix;
    private final BytesRef newStr;
    private final DriverContext driverContext;
    private final ReplaceResultCache cache;
    private Warnings warnings;

    ReplaceConstantOrdinalEvaluator(
        Source source,
        ExpressionEvaluator str,
        Pattern regex,
        byte[] literalPrefix,
        BytesRef newStr,
        ReplaceResultCache cache,
        DriverContext driverContext
    ) {
        this.source = source;
        this.str = str;
        this.regex = regex;
        this.literalPrefix = literalPrefix;
        this.newStr = newStr;
        this.driverContext = driverContext;
        this.cache = cache;
    }

    @Override
    public Block eval(Page page) {
        try (BytesRefBlock strBlock = (BytesRefBlock) str.eval(page)) {
            OrdinalBytesRefBlock ordinals = strBlock.asOrdinals();
            if (ordinals != null && ordinals.isDense() && ordinals.mayHaveMultivaluedFields() == false) {
                Block dictResult = evalDictionary(ordinals);
                if (dictResult != null) {
                    return dictResult;
                }
                // Dictionary path bailed (an entry triggered an exception). Fall through to per-row.
            }
            return evalPerRow(page.getPositionCount(), strBlock);
        }
    }

    /**
     * Apply REPLACE once per dictionary entry and build an {@link OrdinalBytesRefBlock} that re-uses the
     * input ordinals. Returns {@code null} if any dictionary entry throws {@link IllegalArgumentException}
     * — the caller should fall back to per-row evaluation so warnings get attributed at the row level
     * exactly as the legacy path would.
     */
    private Block evalDictionary(OrdinalBytesRefBlock ordinalsBlock) {
        BytesRefVector dictionary = ordinalsBlock.getDictionaryVector();
        int dictSize = dictionary.getPositionCount();
        BytesRefVector newDictionary = null;
        try (BytesRefVector.Builder builder = driverContext.blockFactory().newBytesRefVectorBuilder(dictSize)) {
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < dictSize; i++) {
                BytesRef entry = dictionary.getBytesRef(i, scratch);
                BytesRef replaced;
                try {
                    replaced = process(entry);
                } catch (IllegalArgumentException e) {
                    // Bail to the per-row path so warnings are emitted from the row that triggered the failure
                    // (matching the legacy evaluator's behavior).
                    return null;
                }
                builder.appendBytesRef(replaced);
            }
            newDictionary = builder.build();
        }
        OrdinalBytesRefBlock result = null;
        try {
            IntBlock inputOrdinals = ordinalsBlock.getOrdinalsBlock();
            inputOrdinals.incRef();
            result = new OrdinalBytesRefBlock(inputOrdinals, newDictionary);
            newDictionary = null;
            return result;
        } finally {
            if (result == null) {
                Releasables.closeExpectNoException(newDictionary);
            }
        }
    }

    /**
     * Per-row fallback. Mirrors the loop emitted by the {@code @Evaluator}-generated
     * {@code ReplaceConstantEvaluator} so behavior — nulls, multi-value warnings, and per-row exception
     * handling — matches the legacy path exactly.
     */
    private Block evalPerRow(int positionCount, BytesRefBlock strBlock) {
        try (BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
            BytesRef strScratch = new BytesRef();
            position: for (int p = 0; p < positionCount; p++) {
                switch (strBlock.getValueCount(p)) {
                    case 0:
                        result.appendNull();
                        continue position;
                    case 1:
                        break;
                    default:
                        warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
                        result.appendNull();
                        continue position;
                }
                BytesRef strVal = strBlock.getBytesRef(strBlock.getFirstValueIndex(p), strScratch);
                try {
                    result.appendBytesRef(process(strVal));
                } catch (IllegalArgumentException e) {
                    warnings().registerException(e);
                    result.appendNull();
                }
            }
            return result.build();
        }
    }

    private BytesRef process(BytesRef strVal) {
        if (literalPrefix.length > 0 && Replace.startsWith(strVal, literalPrefix) == false) {
            return strVal;
        }
        if (strVal.length == 0 || strVal.length > ReplaceResultCache.MAX_KEY_LENGTH) {
            return Replace.safeReplace(strVal, regex, newStr);
        }
        int offset = cache.slotOffset(strVal);
        BytesRef cached = cache.get(offset, strVal);
        if (cached != null) {
            return cached;
        }
        BytesRef replaced = Replace.safeReplace(strVal, regex, newStr);
        cache.put(offset, strVal, replaced);
        return replaced;
    }

    @Override
    public long baseRamBytesUsed() {
        // Track the constant byte arrays we hold; the regex Pattern itself is not accounted (matching the
        // convention of the generated ReplaceConstantEvaluator, which also doesn't include the Pattern).
        return BASE_RAM_BYTES_USED + str.baseRamBytesUsed() + RamUsageEstimator.sizeOf(literalPrefix) + RamUsageEstimator.sizeOf(
            newStr.bytes
        );
    }

    @Override
    public String toString() {
        return "ReplaceConstantOrdinalEvaluator[" + "str=" + str + ", regex=" + regex + ", newStr=" + newStr + ", cache=" + cache + "]";
    }

    @Override
    public void close() {
        logger.debug("regex [{}] {}", regex, cache);
        Releasables.closeExpectNoException(str, cache);
    }

    private Warnings warnings() {
        if (warnings == null) {
            this.warnings = driverContext.createWarnings(source);
        }
        return warnings;
    }

    ReplaceResultCache cache() {
        return cache;
    }

    static final class Factory implements ExpressionEvaluator.Factory {
        private final Source source;
        private final ExpressionEvaluator.Factory str;
        private final Pattern regex;
        private final byte[] literalPrefix;
        private final BytesRef newStr;

        Factory(Source source, ExpressionEvaluator.Factory str, Pattern regex, byte[] literalPrefix, BytesRef newStr) {
            this.source = source;
            this.str = str;
            this.regex = regex;
            this.literalPrefix = literalPrefix;
            this.newStr = newStr;
        }

        @Override
        public ReplaceConstantOrdinalEvaluator get(DriverContext context) {
            final ReplaceResultCache cache = new ReplaceResultCache(context.breaker());
            boolean success = false;
            try {
                var evaluator = new ReplaceConstantOrdinalEvaluator(source, str.get(context), regex, literalPrefix, newStr, cache, context);
                success = true;
                return evaluator;
            } finally {
                if (success == false) {
                    cache.close();
                }
            }
        }

        @Override
        public String toString() {
            return "ReplaceConstantOrdinalEvaluator[" + "str=" + str + ", regex=" + regex + ", newStr=" + newStr + "]";
        }
    }

    /**
     * A fixed memoization table: fixed slots, fixed length for each slot, no policy so that cache misses are also cheap.
     */
    static final class ReplaceResultCache implements Releasable {
        static final int SLOTS = 256;
        static final int SLOT_BYTES = 256;
        private static final int HEADER_BYTES = 2;
        static final int MAX_PAYLOAD = SLOT_BYTES - HEADER_BYTES;
        static final int MAX_KEY_LENGTH = MAX_PAYLOAD * 3 / 4;
        private static final int SLOT_SHIFT = Integer.numberOfTrailingZeros(SLOT_BYTES);
        private static final int SLOT_MASK = SLOTS - 1;
        static final long RAM_BYTES_USED = (long) SLOTS * SLOT_BYTES;

        static {
            assert Integer.bitCount(SLOTS) == 1 && Integer.bitCount(SLOT_BYTES) == 1;
            assert MAX_PAYLOAD <= 0xFF;
        }

        private final CircuitBreaker breaker;
        private final byte[] bytes;
        private final BytesRef view;
        private long hits;
        private long misses;

        ReplaceResultCache(CircuitBreaker breaker) {
            breaker.addEstimateBytesAndMaybeBreak(RAM_BYTES_USED, "ReplaceResultCache");
            this.breaker = breaker;
            this.bytes = new byte[SLOTS * SLOT_BYTES];
            this.view = new BytesRef(bytes);
        }

        int slotOffset(BytesRef key) {
            return ((int) MixHash64.hash64(key) & SLOT_MASK) << SLOT_SHIFT;
        }

        BytesRef get(int offset, BytesRef key) {
            assert key.length > 0 && key.length <= MAX_KEY_LENGTH;
            int keyLen = bytes[offset] & 0xFF;
            if (keyLen != key.length
                || Arrays.equals(
                    bytes,
                    offset + HEADER_BYTES,
                    offset + HEADER_BYTES + keyLen,
                    key.bytes,
                    key.offset,
                    key.offset + keyLen
                ) == false) {
                misses++;
                return null;
            }
            hits++;
            view.offset = offset + HEADER_BYTES + keyLen;
            view.length = bytes[offset + 1] & 0xFF;
            return view;
        }

        void put(int offset, BytesRef key, BytesRef value) {
            assert key.length > 0 && key.length <= MAX_KEY_LENGTH;
            if (key.length + value.length > MAX_PAYLOAD) {
                return;
            }
            bytes[offset] = (byte) key.length;
            bytes[offset + 1] = (byte) value.length;
            System.arraycopy(key.bytes, key.offset, bytes, offset + HEADER_BYTES, key.length);
            System.arraycopy(value.bytes, value.offset, bytes, offset + HEADER_BYTES + key.length, value.length);
        }

        @Override
        public void close() {
            breaker.addWithoutBreaking(-RAM_BYTES_USED);
        }

        @Override
        public String toString() {
            return "ReplaceResultCache[hits=" + hits + ", misses=" + misses + "]";
        }
    }
}
