/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.MixHash64;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction.MAX_BYTES_REF_RESULT_SIZE;

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
 *   <li>If replacing any dictionary entry throws {@link IllegalArgumentException} (result-too-large), we abandon the
 *       dictionary path for this page and fall back to per-row evaluation. The fallback emits warnings exactly as the
 *       existing per-row path does.</li>
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
 * <p>
 * The replacement itself is specialised for a constant pattern and replacement. {@code java.util.regex} only supplies
 * match and group positions; the output is assembled directly in UTF-8 from byte ranges of the input and the
 * pre-encoded literal chunks of the replacement template, so no UTF-16 string is ever built. Results are views valid
 * until the next call, which is fine because every caller copies them into a block immediately.
 */
final class ReplaceConstantOrdinalEvaluator implements ExpressionEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ReplaceConstantOrdinalEvaluator.class);
    private static final BytesRef EMPTY = new BytesRef(BytesRef.EMPTY_BYTES);

    private final Source source;
    private final ExpressionEvaluator str;
    private final Pattern regex;
    private final byte[] literalPrefix;
    private final BytesRef newStr;
    private final DriverContext driverContext;
    private Warnings warnings;

    private final Matcher matcher;
    /**
     * Replacement template compiled once with {@link Matcher#appendReplacement} semantics. Segment {@code i} is the group
     * {@code templateGroups[i]} when that is {@code >= 0}, otherwise the UTF-8 literal {@code templateLiterals[i]}.
     */
    private final int[] templateGroups;
    private final byte[][] templateLiterals;
    /** Group index when the template is exactly one group reference, else -1. */
    private final int singleGroup;
    /** {@code appendReplacement} only reports a bad template on the first match, so a compile error is deferred to then. */
    private final IllegalArgumentException templateError;

    private final BytesRef view = new BytesRef();
    private final AsciiSeq asciiSeq = new AsciiSeq();
    private byte[] outBytes = BytesRef.EMPTY_BYTES;
    private int outLen;
    private final ReplaceResultCache cache;

    ReplaceConstantOrdinalEvaluator(
        Source source,
        ExpressionEvaluator str,
        Pattern regex,
        byte[] literalPrefix,
        BytesRef newStr,
        DriverContext driverContext
    ) {
        this.source = source;
        this.str = str;
        this.regex = regex;
        this.literalPrefix = literalPrefix;
        this.newStr = newStr;
        this.driverContext = driverContext;
        this.matcher = regex.matcher("");
        this.cache = new ReplaceResultCache(driverContext.breaker());

        List<byte[]> literals = new ArrayList<>();
        List<Integer> groups = new ArrayList<>();
        IllegalArgumentException error = null;
        try {
            compileTemplate(new String(newStr.bytes, newStr.offset, newStr.length, StandardCharsets.UTF_8), literals, groups);
        } catch (IllegalArgumentException e) {
            error = e;
        }
        this.templateError = error;
        this.templateLiterals = literals.toArray(byte[][]::new);
        this.templateGroups = groups.stream().mapToInt(Integer::intValue).toArray();
        this.singleGroup = templateGroups.length == 1 && templateGroups[0] >= 0 ? templateGroups[0] : -1;
    }

    /** Mirrors the template grammar and error messages of {@link Matcher#appendReplacement}. */
    private void compileTemplate(String replacement, List<byte[]> literals, List<Integer> groups) {
        int groupCount = matcher.groupCount();
        StringBuilder literal = new StringBuilder();
        int cursor = 0;
        while (cursor < replacement.length()) {
            char c = replacement.charAt(cursor);
            if (c == '\\') {
                cursor++;
                if (cursor == replacement.length()) {
                    throw new IllegalArgumentException("character to be escaped is missing");
                }
                literal.append(replacement.charAt(cursor));
                cursor++;
            } else if (c == '$') {
                cursor++;
                if (cursor == replacement.length()) {
                    throw new IllegalArgumentException("Illegal group reference: group index is missing");
                }
                c = replacement.charAt(cursor);
                int refNum;
                if (c == '{') {
                    cursor++;
                    int nameStart = cursor;
                    while (cursor < replacement.length() && isGroupNameChar(replacement.charAt(cursor), cursor == nameStart)) {
                        cursor++;
                    }
                    if (cursor == nameStart) {
                        throw new IllegalArgumentException("named capturing group has 0 length name");
                    }
                    if (cursor == replacement.length() || replacement.charAt(cursor) != '}') {
                        throw new IllegalArgumentException("named capturing group is missing trailing '}'");
                    }
                    String name = replacement.substring(nameStart, cursor);
                    cursor++;
                    Map<String, Integer> named = regex.namedGroups();
                    if (named.containsKey(name) == false) {
                        throw new IllegalArgumentException("No group with name {" + name + "}");
                    }
                    refNum = named.get(name);
                } else {
                    refNum = c - '0';
                    if (refNum < 0 || refNum > 9) {
                        throw new IllegalArgumentException("Illegal group reference");
                    }
                    cursor++;
                    while (cursor < replacement.length()) {
                        int nextDigit = replacement.charAt(cursor) - '0';
                        if (nextDigit < 0 || nextDigit > 9) {
                            break;
                        }
                        int newRefNum = refNum * 10 + nextDigit;
                        if (groupCount < newRefNum) {
                            break;
                        }
                        refNum = newRefNum;
                        cursor++;
                    }
                    if (refNum > groupCount) {
                        throw new IllegalArgumentException("No group " + refNum);
                    }
                }
                if (literal.isEmpty() == false) {
                    literals.add(literal.toString().getBytes(StandardCharsets.UTF_8));
                    groups.add(-1);
                    literal.setLength(0);
                }
                literals.add(null);
                groups.add(refNum);
            } else {
                literal.append(c);
                cursor++;
            }
        }
        if (literal.isEmpty() == false) {
            literals.add(literal.toString().getBytes(StandardCharsets.UTF_8));
            groups.add(-1);
        }
    }

    private static boolean isGroupNameChar(char c, boolean first) {
        boolean letter = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
        return letter || (first == false && c >= '0' && c <= '9');
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

    private BytesRef process(BytesRef in) {
        if (literalPrefix.length > 0 && Replace.startsWith(in, literalPrefix) == false) {
            return in;
        }
        if (in.length == 0 || in.length > ReplaceResultCache.MAX_KEY_LENGTH) {
            return safeReplace(in);
        }
        int slot = cache.slotOffset(in);
        BytesRef cached = cache.get(slot, in);
        if (cached != null) {
            return cached;
        }
        BytesRef replaced = safeReplace(in);
        cache.put(slot, in, replaced);
        return replaced;
    }

    private BytesRef safeReplace(BytesRef in) {
        try {
            return doReplace(in);
        } catch (StackOverflowError e) {
            throw new IllegalArgumentException("Pattern nesting is too deep to evaluate", e);
        }
    }

    private BytesRef doReplace(BytesRef in) {
        // ASCII rows are matched straight over the bytes through a CharSequence view, no decode; char offsets are then byte
        // offsets. Anything else is decoded once and char offsets are mapped back with utf8Length.
        boolean ascii = isAscii(in.bytes, in.offset, in.length);
        String decoded = ascii ? null : new String(in.bytes, in.offset, in.length, StandardCharsets.UTF_8);
        CharSequence s = ascii ? asciiSeq.reset(in) : decoded;
        Matcher m = matcher.reset(s);
        if (m.find() == false) {
            return in;
        }
        if (templateError != null) {
            throw templateError;
        }
        int matchStart = m.start();
        int matchEnd = m.end();
        if (singleGroup >= 0 && matchStart == 0 && matchEnd == s.length()) {
            // The whole input matched, so the result is that group alone. Any further match can only be empty at the end
            // of the input and a group of an empty match contributes nothing, so the general loop would produce the same.
            int groupStart = m.start(singleGroup);
            if (groupStart < 0) {
                return EMPTY;
            }
            int groupEnd = m.end(singleGroup);
            view.bytes = in.bytes;
            if (ascii) {
                view.offset = in.offset + groupStart;
                view.length = groupEnd - groupStart;
            } else {
                view.offset = in.offset + utf8Length(decoded, 0, groupStart);
                view.length = utf8Length(decoded, groupStart, groupEnd);
            }
            return view;
        }
        outLen = 0;
        int charPos = 0;
        int bytePos = 0;
        do {
            matchStart = m.start();
            matchEnd = m.end();
            int matchStartByte = ascii ? matchStart : bytePos + utf8Length(decoded, charPos, matchStart);
            append(in.bytes, in.offset + bytePos, matchStartByte - bytePos);
            for (int i = 0; i < templateGroups.length; i++) {
                int group = templateGroups[i];
                if (group < 0) {
                    byte[] literal = templateLiterals[i];
                    append(literal, 0, literal.length);
                    continue;
                }
                int groupStart = m.start(group);
                if (groupStart < 0) {
                    continue;
                }
                int groupEnd = m.end(group);
                if (ascii) {
                    append(in.bytes, in.offset + groupStart, groupEnd - groupStart);
                } else {
                    // Groups inside look-behinds can lie before the match, so map from the start of the string.
                    append(in.bytes, in.offset + utf8Length(decoded, 0, groupStart), utf8Length(decoded, groupStart, groupEnd));
                }
            }
            bytePos = ascii ? matchEnd : matchStartByte + utf8Length(decoded, matchStart, matchEnd);
            charPos = matchEnd;
        } while (m.find());
        append(in.bytes, in.offset + bytePos, in.length - bytePos);
        view.bytes = outBytes;
        view.offset = 0;
        view.length = outLen;
        return view;
    }

    private void append(byte[] src, int offset, int length) {
        if (outLen + length > MAX_BYTES_REF_RESULT_SIZE) {
            throw new IllegalArgumentException(
                "Creating strings with more than [" + MAX_BYTES_REF_RESULT_SIZE + "] bytes is not supported"
            );
        }
        if (outLen + length > outBytes.length) {
            outBytes = ArrayUtil.grow(outBytes, outLen + length);
        }
        System.arraycopy(src, offset, outBytes, outLen, length);
        outLen += length;
    }

    /** UTF-8 length of {@code s[from, to)}. Typed on {@link String} so {@code charAt} is a direct, inlinable call. */
    private static int utf8Length(String s, int from, int to) {
        int bytes = 0;
        for (int i = from; i < to; i++) {
            char c = s.charAt(i);
            if (c < 0x80) {
                bytes += 1;
            } else if (c < 0x800) {
                bytes += 2;
            } else if (Character.isHighSurrogate(c) && i + 1 < to && Character.isLowSurrogate(s.charAt(i + 1))) {
                bytes += 4;
                i++;
            } else {
                bytes += 3;
            }
        }
        return bytes;
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
        return "ReplaceConstantOrdinalEvaluator[" + "str=" + str + ", regex=" + regex + ", newStr=" + newStr + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(str, cache);
    }

    private Warnings warnings() {
        if (warnings == null) {
            this.warnings = driverContext.createWarnings(source);
        }
        return warnings;
    }

    private static final long HIGH_BITS = 0x8080808080808080L;

    /** Eight bytes at a time through the high-bit mask, then the tail byte by byte. */
    static boolean isAscii(byte[] bytes, int offset, int length) {
        int i = offset;
        int end = offset + length;
        for (int wordEnd = end - Long.BYTES; i <= wordEnd; i += Long.BYTES) {
            if (((long) BitUtil.VH_LE_LONG.get(bytes, i) & HIGH_BITS) != 0) {
                return false;
            }
        }
        for (; i < end; i++) {
            if (bytes[i] < 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Zero-copy {@link CharSequence} over ASCII bytes, so the regex engine reads the input in place instead of a decoded
     * copy. Only valid for bytes that passed {@link #isAscii}, where {@code (char) b} is the exact decode.
     */
    static final class AsciiSeq implements CharSequence {
        private byte[] bytes;
        private int offset;
        private int length;

        AsciiSeq reset(BytesRef ref) {
            this.bytes = ref.bytes;
            this.offset = ref.offset;
            this.length = ref.length;
            return this;
        }

        @Override
        public int length() {
            return length;
        }

        @Override
        public char charAt(int index) {
            return (char) bytes[offset + index];
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return new String(bytes, offset + start, end - start, StandardCharsets.ISO_8859_1);
        }

        @Override
        public String toString() {
            return new String(bytes, offset, length, StandardCharsets.ISO_8859_1);
        }
    }

    /**
     * Direct-mapped memoization table for the repeated head of the input: one fixed 256-byte slot per hash index holding
     * {@code [keyLen][valueLen][key][value]}, key verified byte-for-byte on lookup, miss overwrites. Fixed 64 KB per
     * evaluator, accounted once against the breaker. Short repeated inputs still pay the engine's full fixed per-row cost,
     * so a hit on them is worth nearly a whole row.
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
                return null;
            }
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
            return new ReplaceConstantOrdinalEvaluator(source, str.get(context), regex, literalPrefix, newStr, context);
        }

        @Override
        public String toString() {
            return "ReplaceConstantOrdinalEvaluator[" + "str=" + str + ", regex=" + regex + ", newStr=" + newStr + "]";
        }
    }
}
