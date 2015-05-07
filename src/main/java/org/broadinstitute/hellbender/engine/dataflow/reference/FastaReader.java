package org.broadinstitute.hellbender.engine.dataflow.reference;

/**
 * Created by davidada on 5/7/15.
 */

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedHashMap;
import java.util.Map;

public class FastaReader implements Closeable {

    public interface Contig {

        String get(int beginIndex, int endIndex);

        int length();

        String name();
    }

    private static class ContigImpl implements Contig {

        private final String name;
        private final int length;
        private final int bases;
        private final int bytes;
        private final ByteBuffer buffer;

        ContigImpl(String name, int length, int bases, int bytes, ByteBuffer buffer) {
            this.name = name;
            this.length = length;
            this.bases = bases;
            this.bytes = bytes;
            this.buffer = buffer;
        }

        @Override
        public String get(int beginIndex, int endIndex) {
            StringBuilder builder = new StringBuilder();
            for (int end = shift(endIndex), i = shift(beginIndex); i < end; ++i) {
                int codePoint = buffer.get(i);
                if (!Character.isWhitespace(codePoint)) {
                    builder.appendCodePoint(codePoint);
                }
            }
            return builder.toString();
        }

        @Override
        public int length() {
            return length;
        }

        @Override
        public String name() {
            return name;
        }

        private int shift(int i) {
            return bytes * (i / bases) + i % bases;
        }
    }

    public static FastaReader create(File fastaFile) throws IOException {
        File faiFile = fastaFile.toPath()
                .getParent()
                .resolve(String.format("%s.fai", fastaFile.getName()))
                .toFile();
        return faiFile.exists() && faiFile.isFile() && faiFile.canRead()
                ? create(fastaFile, faiFile)
                : FastaReader.of(fastaFile, FastaIndex.create(fastaFile));
    }

    public static FastaReader create(File fastaFile, File faiFile) throws IOException {
        return FastaReader.of(fastaFile, FastaIndex.read(faiFile));
    }

    private static FastaReader of(File fastaFile, FastaIndex index) throws IOException {
        RandomAccessFile file = new RandomAccessFile(fastaFile, "r");
        FileChannel channel = file.getChannel();
        LinkedHashMap<String, Contig> contigs = new LinkedHashMap<>();
        for (FastaIndex.Entry entry : index.entries()) {
            String name = entry.name();
            int length = entry.length(), bases = entry.bases(), bytes = entry.bytes();
            contigs.put(name, new ContigImpl(name, length, bases, bytes, channel.map(
                    FileChannel.MapMode.READ_ONLY, entry.offset(),
                    bytes * (length / bases) + length % bases)));
        }
        return new FastaReader(file, channel, contigs);
    }

    private final Map<String, Contig> contigs;
    private final FileChannel fileChannel;
    private final RandomAccessFile randomAccessFile;

    private FastaReader(
            RandomAccessFile randomAccessFile,
            FileChannel fileChannel,
            Map<String, Contig> contigs) {
        this.randomAccessFile = randomAccessFile;
        this.fileChannel = fileChannel;
        this.contigs = contigs;
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
        randomAccessFile.close();
    }

    public Map<String, Contig> contigs() {
        return contigs;
    }
}
