/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.compress.LZ4;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.elasticsearch.core.IOUtils;

import java.io.Closeable;
import java.io.IOException;

class ES817TSDBCompressingBinaryDocValues {
    static final String TMP_BLOCK_POINTERS_CODEC = "TSDB_817_BlockPointers";

    static final class Entry {
        // DISI
        long indexedDISIOffset;
        long indexedDISILength;
        short jumpTableEntryCount;
        byte denseRankPower;

        // block offsets
        int numDocsWithValues;
        long blockAddressOffset;
        long blockAddressLength;
        DirectMonotonicReader.Meta blockAddressMeta;

        // values
        long dataOffset;
        long dataLength;
    }

    static Entry readEntry(IndexInput meta) throws IOException {
        Entry entry = new Entry();
        entry.dataOffset = meta.readLong();
        entry.dataLength = meta.readVLong();
        entry.indexedDISIOffset = meta.readLong();
        if (entry.indexedDISIOffset >= 0) {
            entry.indexedDISILength = meta.readVLong();
            entry.jumpTableEntryCount = meta.readShort();
            entry.denseRankPower = meta.readByte();
        }
        // block addresses
        entry.numDocsWithValues = meta.readVInt();
        entry.blockAddressOffset = meta.readLong();
        final int blockShift = meta.readVInt();
        entry.blockAddressMeta = DirectMonotonicReader.loadMeta(meta, entry.numDocsWithValues + 1, blockShift);
        entry.blockAddressLength = meta.readVLong();
        return entry;
    }

    static final class Writer implements Closeable {
        final SegmentWriteState state;
        final IndexOutput data;
        final IndexOutput meta;
        final LZ4.FastCompressionHashTable ht = new LZ4.FastCompressionHashTable();

        int totalDocsWithValues = 0;

        private IndexOutput tmpBlockPointers;

        Writer(SegmentWriteState state, IndexOutput data, IndexOutput meta) throws IOException {
            this.state = state;
            this.data = data;
            this.meta = meta;
            tmpBlockPointers = state.directory.createTempOutput(state.segmentInfo.name, "tsdb_block_pointers", state.context);
            boolean success = false;
            try {
                CodecUtil.writeHeader(tmpBlockPointers, TMP_BLOCK_POINTERS_CODEC, ES817TSDBDocValuesFormat.VERSION_CURRENT);
                success = true;
            } finally {
                if (success == false) {
                    tmpBlockPointers.close();
                }
            }
        }

        void add(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            BinaryDocValues values = valuesProducer.getBinary(field);
            final long dataStartFP = data.getFilePointer();
            for (int docID = values.nextDoc(); docID != DocIdSetIterator.NO_MORE_DOCS; docID = values.nextDoc()) {
                totalDocsWithValues++;
                addDoc(values.binaryValue());
            }
            meta.writeLong(dataStartFP); // dataOffset
            meta.writeVLong(data.getFilePointer() - dataStartFP); // dataLength
            if (totalDocsWithValues == 0) {
                meta.writeLong(-2); // indexedDISIOffset
            } else if (totalDocsWithValues == state.segmentInfo.maxDoc()) {
                meta.writeLong(-1); // indexedDISIOffset
            } else {
                long offset = data.getFilePointer(); // We can store
                meta.writeLong(offset); // indexedDISIOffset
                values = valuesProducer.getBinary(field);
                final short jumpTableEntryCount = IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
                long indexedDISILength = data.getFilePointer() - offset;
                meta.writeVLong(indexedDISILength); // indexedDISILength
                meta.writeShort(jumpTableEntryCount);
                meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
            }
            writeMetadata();
        }

        void addDoc(BytesRef v) throws IOException {
            if (v.length >= 1024) {
                data.writeByte((byte) 1);
                long fp = data.getFilePointer();
                data.writeVInt(v.length);
                LZ4.compress(v.bytes, v.offset, v.length, data, ht);
                tmpBlockPointers.writeVInt(Math.toIntExact(data.getFilePointer() - fp));
            } else {
                data.writeByte((byte) 0);
                data.writeBytes(v.bytes, v.offset, v.length);
                tmpBlockPointers.writeVInt(v.length);
            }
        }

        void writeMetadata() throws IOException {
            CodecUtil.writeFooter(tmpBlockPointers);
            String fileName = tmpBlockPointers.getName();
            try {
                tmpBlockPointers.close();
                try (var blockPointerIn = state.directory.openChecksumInput(fileName)) {
                    CodecUtil.checkHeader(
                        blockPointerIn,
                        TMP_BLOCK_POINTERS_CODEC,
                        ES817TSDBDocValuesFormat.VERSION_CURRENT,
                        ES817TSDBDocValuesFormat.VERSION_CURRENT
                    );
                    Throwable priorE = null;
                    try {
                        final long blockAddressesStart = data.getFilePointer();
                        meta.writeVInt(totalDocsWithValues);
                        meta.writeLong(blockAddressesStart);
                        meta.writeVInt(ES817TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT);
                        final DirectMonotonicWriter blockPointers = DirectMonotonicWriter.getInstance(
                            meta,
                            data,
                            totalDocsWithValues + 1,
                            ES817TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT
                        );
                        long addr = 0;
                        blockPointers.add(addr);
                        for (int i = 0; i < totalDocsWithValues; i++) {
                            addr += blockPointerIn.readVInt() + 1;
                            blockPointers.add(addr);
                        }
                        blockPointers.finish();
                        final long blockAddressesLength = data.getFilePointer() - blockAddressesStart;
                        meta.writeVLong(blockAddressesLength);
                    } catch (Throwable e) {
                        priorE = e;
                    } finally {
                        CodecUtil.checkFooter(blockPointerIn, priorE);
                    }
                }
            } finally {
                this.tmpBlockPointers = null;
                state.directory.deleteFile(fileName);
            }
        }

        @Override
        public void close() throws IOException {
            if (tmpBlockPointers != null) {
                IOUtils.close(tmpBlockPointers, () -> state.directory.deleteFile(tmpBlockPointers.getName()));
            }
        }
    }

    static final class Reader {
        final LongValues blockAddresses;
        final IndexInput data;
        final BytesRef values = new BytesRef();

        Reader(Entry entry, IndexInput data, boolean merging) throws IOException {
            final RandomAccessInput addressesData = data.randomAccessSlice(entry.blockAddressOffset, entry.blockAddressLength);
            this.blockAddresses = DirectMonotonicReader.getInstance(entry.blockAddressMeta, addressesData, merging);
            this.data = data.slice("binary_dv", entry.dataOffset, entry.dataLength);
        }

        BytesRef readValue(int docID) throws IOException {
            final long address = blockAddresses.get(docID);
            data.seek(address);
            byte header = data.readByte();
            if (header == 0) {
                values.length = Math.toIntExact(blockAddresses.get(docID + 1) - address - 1);
                values.bytes = ArrayUtil.growNoCopy(values.bytes, values.length + 2);
                data.readBytes(values.bytes, 0, values.length);
            } else {
                values.length = data.readVInt();
                values.bytes = ArrayUtil.growNoCopy(values.bytes, values.length);
                LZ4.decompress(data, values.length, values.bytes, 0);
            }
            return values;
        }
    }
}
