/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.hash.BufferedMurmur3Hasher;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.index.mapper.RoutingPathFields;
import org.elasticsearch.xcontent.XContentString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A builder for creating time series identifiers (TSIDs) based on dimensions.
 * This class allows adding various types of dimensions (int, long, double, boolean, string, bytes)
 * and builds a TSID that is a hash of the dimension names and values.
 * Important properties of TSIDs are that they cluster similar time series together,
 * which helps with storage efficiency,
 * and that they minimize the risk of hash collisions.
 * At the same time, they should be short to be efficient in terms of storage and processing.
 */
public class TsidBuilder {

    private static final int NAME_HASH_BITS = 3;
    private static final int FIRST_VALUE_HASH_BITS = 3;
    private static final int SIM_HASH_BITS = 10;
    private static final int NUM_CLUSTERING_VALUES = 3; // V2, V3, V4 get 8 bits each after the prefix
    private final BufferedMurmur3Hasher murmur3Hasher = new BufferedMurmur3Hasher(0L);

    private final List<Dimension> dimensions;

    public TsidBuilder() {
        this.dimensions = new ArrayList<>();
    }

    public TsidBuilder(int size) {
        this.dimensions = new ArrayList<>(size);
    }

    public static TsidBuilder newBuilder() {
        return new TsidBuilder();
    }

    /**
     * Adds an integer dimension to the TSID.
     *
     * @param path  the path of the dimension
     * @param value the integer value of the dimension
     * @return the TsidBuilder instance for method chaining
     */
    public TsidBuilder addIntDimension(String path, int value) {
        addDimension(path, new MurmurHash3.Hash128(1, value));
        return this;
    }

    /**
     * Adds a long dimension to the TSID.
     *
     * @param path  the path of the dimension
     * @param value the long value of the dimension
     * @return the TsidBuilder instance for method chaining
     */
    public TsidBuilder addLongDimension(String path, long value) {
        addDimension(path, new MurmurHash3.Hash128(1, value));
        return this;
    }

    /**
     * Adds a double dimension to the TSID.
     *
     * @param path  the path of the dimension
     * @param value the double value of the dimension
     * @return the TsidBuilder instance for method chaining
     */
    public TsidBuilder addDoubleDimension(String path, double value) {
        addDimension(path, new MurmurHash3.Hash128(2, Double.doubleToLongBits(value)));
        return this;
    }

    /**
     * Adds a boolean dimension to the TSID.
     *
     * @param path  the path of the dimension
     * @param value the boolean value of the dimension
     * @return the TsidBuilder instance for method chaining
     */
    public TsidBuilder addBooleanDimension(String path, boolean value) {
        addDimension(path, new MurmurHash3.Hash128(3, value ? 1 : 0));
        return this;
    }

    /**
     * Adds a string dimension to the TSID.
     *
     * @param path  the path of the dimension
     * @param value the string value of the dimension
     * @return the TsidBuilder instance for method chaining
     */
    public TsidBuilder addStringDimension(String path, String value) {
        addStringDimension(path, new BytesRef(value));
        return this;
    }

    private void addStringDimension(String path, BytesRef value) {
        addStringDimension(path, value.bytes, value.offset, value.length);
    }

    /**
     * Adds a string dimension to the TSID.
     *
     * @param path  the path of the dimension
     * @param value the UTF8Bytes value of the dimension
     * @return the TsidBuilder instance for method chaining
     */
    public TsidBuilder addStringDimension(String path, XContentString.UTF8Bytes value) {
        addStringDimension(path, value.bytes(), value.offset(), value.length());
        return this;
    }

    /**
     * Adds a string dimension to the TSID using a byte array.
     * The value is provided as UTF-8 encoded bytes[].
     *
     * @param path the path of the dimension
     * @param utf8Bytes the UTF-8 encoded bytes of the string value
     * @param offset the offset in the byte array where the string starts
     * @param length the length of the string in bytes
     * @return the TsidBuilder instance for method chaining
     */
    public TsidBuilder addStringDimension(String path, byte[] utf8Bytes, int offset, int length) {
        murmur3Hasher.reset();
        murmur3Hasher.update(utf8Bytes, offset, length);
        MurmurHash3.Hash128 hash128 = murmur3Hasher.digestHash();
        addDimension(path, hash128);
        return this;
    }

    /**
     * Adds a value to the TSID using a funnel.
     * This allows for complex types to be added to the TSID.
     *
     * @param value  the value to add
     * @param funnel the funnel that describes how to add the value
     * @param <T>    the type of the value
     * @return the TsidBuilder instance for method chaining
     */
    public <T> TsidBuilder add(T value, TsidFunnel<T> funnel) {
        funnel.add(value, this);
        return this;
    }

    /**
     * Adds a value to the TSID using a funnel that can throw exceptions.
     * This allows for complex types to be added to the TSID.
     *
     * @param value  the value to add
     * @param funnel the funnel that describes how to add the value
     * @param <T>    the type of the value
     * @param <E>    the type of exception that can be thrown
     * @return the TsidBuilder instance for method chaining
     * @throws E if an exception occurs while adding the value
     */
    public <T, E extends Exception> TsidBuilder add(T value, ThrowingTsidFunnel<T, E> funnel) throws E {
        funnel.add(value, this);
        return this;
    }

    private void addDimension(String path, MurmurHash3.Hash128 valueHash) {
        murmur3Hasher.reset();
        murmur3Hasher.addString(path);
        MurmurHash3.Hash128 pathHash = murmur3Hasher.digestHash();
        dimensions.add(new Dimension(path, pathHash, valueHash, dimensions.size()));
    }

    /**
     * Adds all dimensions from another TsidBuilder to this one.
     * If the other builder is null or has no dimensions, this method does nothing.
     *
     * @param other the other TsidBuilder to add dimensions from
     * @return this TsidBuilder instance for method chaining
     */
    public TsidBuilder addAll(TsidBuilder other) {
        if (other == null || other.dimensions.isEmpty()) {
            return this;
        }
        dimensions.addAll(other.dimensions);
        return this;
    }

    /**
     * Computes the hash of the dimensions added to this builder.
     * The hash is a 128-bit value that is computed based on the dimension names and values.
     *
     * @return a HashValue128 representing the hash of the dimensions
     * @throws IllegalArgumentException if no dimensions have been added
     */
    public MurmurHash3.Hash128 hash() {
        Collections.sort(dimensions);
        murmur3Hasher.reset();
        for (Dimension dim : dimensions) {
            murmur3Hasher.addLongs(dim.pathHash.h1, dim.pathHash.h2, dim.valueHash.h1, dim.valueHash.h2);
        }
        return murmur3Hasher.digestHash();
    }

    /**
     * Builds a time series identifier (TSID) based on the dimensions added to this builder.
     * This is a slight adaptation of {@link RoutingPathFields#buildHash()} but creates shorter tsids.
     * The TSID is a hash that includes:
     * <ul>
     *     <li>
     *         Bytes 0-1 — 16-bit partitioning prefix: 3-bit name hash + 3-bit first value hash
     *         + 10-bit SimHash of remaining dimensions.
     *     </li>
     *     <li>
     *         Bytes 2-4 — 8-bit hash each for dimensions 2, 3, 4 (V2, V3, V4) for clustering.
     *         These restore hierarchical sort order so similar time series remain adjacent.
     *     </li>
     *     <li>
     *         Bytes 5-20 — a hash of all names and values combined (16 bytes) to avoid hash collisions.
     *     </li>
     * </ul>
     *
     * @return a BytesRef containing the TSID
     * @throws IllegalArgumentException if no dimensions have been added
     */
    public BytesRef buildTsid() {
        throwIfEmpty();
        Collections.sort(dimensions);

        int numClusteringValues = Math.min(NUM_CLUSTERING_VALUES, Math.max(0, dimensions.size() - 1));
        byte[] hash = new byte[2 + numClusteringValues + 16];
        int index = 0;
        int prefix = partitioningPrefix();
        hash[index++] = (byte) (prefix >>> 8);
        hash[index++] = (byte) prefix;
        MurmurHash3.Hash128 hashBuffer = new MurmurHash3.Hash128();
        for (int i = 1; i <= numClusteringValues; i++) {
            Dimension dim = dimensions.get(i);
            murmur3Hasher.reset();
            murmur3Hasher.addLong(dim.valueHash().h1 ^ dim.valueHash().h2);
            hash[index++] = (byte) murmur3Hasher.digestHash(hashBuffer).h1;
        }
        murmur3Hasher.reset();
        for (int i = 0; i < dimensions.size(); i++) {
            Dimension dim = dimensions.get(i);
            murmur3Hasher.addLongs(dim.pathHash.h1, dim.pathHash.h2, dim.valueHash.h1, dim.valueHash.h2);
        }
        index = writeHash128(murmur3Hasher.digestHash(hashBuffer), hash, index);
        return new BytesRef(hash, 0, index);
    }

    private void throwIfEmpty() {
        if (dimensions.isEmpty()) {
            throw new IllegalArgumentException("Dimensions are empty");
        }
    }

    /**
     * Computes a 16-bit partitioning prefix: 3-bit name hash + 3-bit V1 hash + 10-bit SimHash(dims 2+).
     */
    private int partitioningPrefix() {
        // name hash
        murmur3Hasher.reset();
        for (Dimension dim : dimensions) {
            murmur3Hasher.addLong(dim.pathHash().h1 ^ dim.pathHash().h2);
        }
        int nameHash = (int) murmur3Hasher.digestHash().h1 & ((1 << NAME_HASH_BITS) - 1);

        // first value hash
        Dimension first = dimensions.getFirst();
        murmur3Hasher.reset();
        murmur3Hasher.addLong(first.valueHash().h1 ^ first.valueHash().h2);
        int valueHash = (int) murmur3Hasher.digestHash().h1 & ((1 << FIRST_VALUE_HASH_BITS) - 1);

        // SimHash of remaining dimensions (2+)
        int simHash = simHash();

        return (nameHash << (FIRST_VALUE_HASH_BITS + SIM_HASH_BITS)) | (valueHash << SIM_HASH_BITS) | simHash;
    }

    /**
     * SimHash (locality-sensitive hash) across dimensions 2+.
     * Each dimension votes +1 or -1 per bit based on its value hash.
     */
    private int simHash() {
        int[] votes = new int[SIM_HASH_BITS];
        for (int d = 1; d < dimensions.size(); d++) {
            Dimension dim = dimensions.get(d);
            long h = dim.valueHash().h1 ^ dim.valueHash().h2;
            for (int bit = 0; bit < SIM_HASH_BITS; bit++) {
                votes[bit] += ((h >>> bit) & 1) == 1 ? 1 : -1;
            }
        }
        int result = 0;
        for (int bit = 0; bit < SIM_HASH_BITS; bit++) {
            if (votes[bit] > 0) {
                result |= (1 << bit);
            }
        }
        return result;
    }

    private static int writeHash128(MurmurHash3.Hash128 hash128, byte[] buffer, int index) {
        ByteUtils.writeLongLE(hash128.h2, buffer, index);
        index += 8;
        ByteUtils.writeLongLE(hash128.h1, buffer, index);
        index += 8;
        return index;
    }

    public int size() {
        return dimensions.size();
    }

    /**
     * A functional interface that describes how objects of a complex type are added to a TSID.
     *
     * @param <T> the type of the value
     */
    @FunctionalInterface
    public interface TsidFunnel<T> {
        void add(T value, TsidBuilder tsidBuilder);
    }

    /**
     * A functional interface that describes how objects of a complex type are added to a TSID,
     * allowing for exceptions to be thrown during the process.
     *
     * @param <T> the type of the value
     * @param <E> the type of exception that can be thrown
     */
    @FunctionalInterface
    public interface ThrowingTsidFunnel<T, E extends Exception> {
        void add(T value, TsidBuilder tsidBuilder) throws E;
    }

    private record Dimension(String path, MurmurHash3.Hash128 pathHash, MurmurHash3.Hash128 valueHash, int insertionOrder)
        implements
            Comparable<Dimension> {
        @Override
        public int compareTo(Dimension o) {
            int i = path.compareTo(o.path);
            if (i != 0) return i;
            // ensures array values are in the order as they appear in the source
            return Integer.compare(insertionOrder, o.insertionOrder);
        }
    }
}
