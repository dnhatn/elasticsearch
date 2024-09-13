/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterBinaryDocValues;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OneMergeWrappingMergePolicy;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;

final class RecoverySourcePruneMergePolicy extends OneMergeWrappingMergePolicy {
    RecoverySourcePruneMergePolicy(
        String recoverySourceField,
        boolean pruneIdField,
        Supplier<Query> retainSourceQuerySupplier,
        MergePolicy in
    ) {
        super(in, toWrap -> new OneMerge(toWrap.segments) {
            @Override
            public CodecReader wrapForMerge(CodecReader reader) throws IOException {
                CodecReader wrapped = toWrap.wrapForMerge(reader);
                return wrapReader(recoverySourceField, pruneIdField, wrapped, retainSourceQuerySupplier);
            }
        });
    }

    private static CodecReader wrapReader(
        String recoverySourceField,
        boolean pruneIdField,
        CodecReader reader,
        Supplier<Query> retainSourceQuerySupplier
    ) throws IOException {
        var recoverySource = reader.getBinaryDocValues(recoverySourceField);
        if (recoverySource == null || recoverySource.nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
            return reader; // early terminate - nothing to do here since non of the docs has a recovery source anymore.
        }
        IndexSearcher s = new IndexSearcher(reader);
        s.setQueryCache(null);
        Weight weight = s.createWeight(s.rewrite(retainSourceQuerySupplier.get()), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        Scorer scorer = weight.scorer(reader.getContext());
        if (scorer != null) {
            BitSet recoverySourceToKeep = BitSet.of(scorer.iterator(), reader.maxDoc());
            // calculating the cardinality is significantly cheaper than skipping all bulk-merging we might do
            // if retentions are high we keep most of it
            if (recoverySourceToKeep.cardinality() == reader.maxDoc()) {
                return reader; // keep all source
            }
            return new SourcePruningFilterCodecReader(recoverySourceField, pruneIdField, reader, recoverySourceToKeep);
        } else {
            return new SourcePruningFilterCodecReader(recoverySourceField, pruneIdField, reader, null);
        }
    }

    private static class SourcePruningFilterCodecReader extends FilterCodecReader {
        private final BitSet recoverySourceToKeep;
        private final String recoverySourceField;
        private final boolean pruneIdField;

        SourcePruningFilterCodecReader(String recoverySourceField, boolean pruneIdField, CodecReader reader, BitSet recoverySourceToKeep) {
            super(reader);
            this.recoverySourceField = recoverySourceField;
            this.recoverySourceToKeep = recoverySourceToKeep;
            this.pruneIdField = pruneIdField;
        }

        @Override
        public DocValuesProducer getDocValuesReader() {
            DocValuesProducer docValuesReader = super.getDocValuesReader();
            return new FilterDocValuesProducer(docValuesReader) {
                @Override
                public BinaryDocValues getBinary(FieldInfo field) throws IOException {
                    if (recoverySourceField.equals(field.name) == false) {
                        return super.getBinary(field);
                    }
                    if (recoverySourceToKeep == null) {
                        return DocValues.emptyBinary();
                    }
                    BinaryDocValues binary = super.getBinary(field);
                    var intersection = ConjunctionUtils.intersectIterators(
                        Arrays.asList(binary, new BitSetIterator(recoverySourceToKeep, recoverySourceToKeep.length()))
                    );
                    return new FilterBinaryDocValues(binary) {
                        @Override
                        public int nextDoc() throws IOException {
                            return intersection.nextDoc();
                        }

                        @Override
                        public int advance(int target) throws IOException {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public boolean advanceExact(int target) throws IOException {
                            throw new UnsupportedOperationException();
                        }
                    };
                }
            };
        }
        @Override
        public CacheHelper getCoreCacheHelper() {
            return null;
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return null;
        }

        private static class FilterDocValuesProducer extends DocValuesProducer {
            private final DocValuesProducer in;

            FilterDocValuesProducer(DocValuesProducer in) {
                this.in = in;
            }

            @Override
            public NumericDocValues getNumeric(FieldInfo field) throws IOException {
                return in.getNumeric(field);
            }

            @Override
            public BinaryDocValues getBinary(FieldInfo field) throws IOException {
                return in.getBinary(field);
            }

            @Override
            public SortedDocValues getSorted(FieldInfo field) throws IOException {
                return in.getSorted(field);
            }

            @Override
            public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
                return in.getSortedNumeric(field);
            }

            @Override
            public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
                return in.getSortedSet(field);
            }

            @Override
            public void checkIntegrity() throws IOException {
                in.checkIntegrity();
            }

            @Override
            public void close() throws IOException {
                in.close();
            }
        }
    }
}
