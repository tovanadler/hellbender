package org.broadinstitute.hellbender.engine.dataflow.reference;

/**
 * Created by davidada on 5/7/15.
 */

import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An abstraction for parsing and making use of FASTA index files.
 */
public class FastaIndex {

    public static final class Entry implements Comparable<Entry> {

        private static final Comparator<Entry> COMPARATOR = Ordering.compound(Arrays.asList(
                Ordering.natural().onResultOf(
                        new Function<Entry, Long>() {
                            @Override
                            public Long apply(Entry entry) {
                                return entry.offset();
                            }
                        }),
                Ordering.natural().onResultOf(
                        new Function<Entry, String>() {
                            @Override
                            public String apply(Entry entry) {
                                return entry.name();
                            }
                        }),
                Ordering.natural().onResultOf(
                        new Function<Entry, Integer>() {
                            @Override
                            public Integer apply(Entry entry) {
                                return entry.length();
                            }
                        }),
                Ordering.natural().onResultOf(
                        new Function<Entry, Integer>() {
                            @Override
                            public Integer apply(Entry entry) {
                                return entry.bases();
                            }
                        }),
                Ordering.natural().onResultOf(
                        new Function<Entry, Integer>() {
                            @Override
                            public Integer apply(Entry entry) {
                                return entry.bytes();
                            }
                        })));

        private static final Pattern PATTERN = Pattern.compile(
                "([^\t]+?)\t(0|[1-9][0-9]*?)\t(0|[1-9][0-9]*?)\t(0|[1-9][0-9]*?)\t(0|[1-9][0-9]*?)");

        public static Entry create(String name, int length, long offset, int bases, int bytes) {
            return new Entry(name, length, offset, bases, bytes);
        }

        public static Entry parse(String line) {
            Matcher matcher = PATTERN.matcher(line);
            if (!matcher.matches()) {
                throw new IllegalArgumentException(
                        String.format("Couldn't match \"%s\" against \"%s\"", line, PATTERN.pattern()));
            }
            return create(
                    matcher.group(1),
                    Integer.parseInt(matcher.group(2)),
                    Long.parseLong(matcher.group(3)),
                    Integer.parseInt(matcher.group(4)),
                    Integer.parseInt(matcher.group(5)));
        }

        private final int bases;
        private final int bytes;
        private final int length;
        private final String name;
        private final long offset;

        private Entry(String name, int length, long offset, int bases, int bytes) {
            this.name = name;
            this.length = length;
            this.offset = offset;
            this.bases = bases;
            this.bytes = bytes;
        }

        public int bases() {
            return bases;
        }

        public int bytes() {
            return bytes;
        }

        @Override
        public int compareTo(Entry rhs) {
            return COMPARATOR.compare(this, rhs);
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj
                    || null != obj
                    && Entry.class == obj.getClass()
                    && 0 == compareTo((Entry) obj);
        }

        private Object[] fields() {
            return new Object[]{name(), length(), offset(), bases(), bytes()};
        }

        @Override
        public int hashCode() {
            return Objects.hash(fields());
        }

        public int length() {
            return length;
        }

        public String name() {
            return name;
        }

        public long offset() {
            return offset;
        }

        @Override
        public String toString() {
            return Joiner.on('\t').join(fields());
        }
    }

    private static final Logger LOGGER = Logger.getLogger(FastaIndex.class.getName());

    private static final int NEWLINE_SIZE_IN_BYTES =
            StandardCharsets.UTF_8.encode(CharBuffer.wrap(String.format("%n"))).limit();

    public static FastaIndex create(File fastaFile) throws IOException {
        class ExceptionWrapper extends RuntimeException {

            ExceptionWrapper(IOException cause) {
                super(cause);
            }

            @Override
            public synchronized IOException getCause() {
                return (IOException) super.getCause();
            }
        }
        try (final BufferedReader in = new BufferedReader(new FileReader(fastaFile))) {
            ImmutableSortedSet.Builder<FastaIndex.Entry> entries = ImmutableSortedSet.naturalOrder();
            String name = null;
            int length = 0;
            long offset1 = 0;
            long offset2 = 0;
            int bases = 0;
            for (
                    PeekingIterator<String> iterator = Iterators.peekingIterator(
                            new AbstractIterator<String>() {
                                @Override
                                protected String computeNext() {
                                    try {
                                        String line = in.readLine();
                                        return null == line ? endOfData() : line;
                                    } catch (IOException e) {
                                        throw new ExceptionWrapper(e);
                                    }
                                }
                            });
                    iterator.hasNext(); ) {
                String line = iterator.next();
                int lineLength = line.length();
                offset2 += lineLength + NEWLINE_SIZE_IN_BYTES;
                if (line.startsWith(">")) {
                    if (null != name) {
                        entries.add(FastaIndex.Entry.create(
                                name, length, offset1, bases, bases + NEWLINE_SIZE_IN_BYTES));
                    }
                    name = line.substring(1).split("\\p{Space}+?")[0];
                    length = 0;
                    offset1 = offset2;
                    bases = 0;
                } else {
                    if (0 == bases) {
                        bases = lineLength;
                    } else if (bases != lineLength
                            && iterator.hasNext()
                            && !iterator.peek().startsWith(">")) {
                        LOGGER.warning(String.format(
                                "Inconsistent line lengths at contig \"%s\", offset %d",
                                name,
                                offset2 - lineLength - NEWLINE_SIZE_IN_BYTES));
                    }
                    length += lineLength;
                }
            }
            if (null != name) {
                entries.add(FastaIndex.Entry.create(
                        name, length, offset1, bases, bases + NEWLINE_SIZE_IN_BYTES));
            }
            return createFromEntries(entries.build());
        } catch (ExceptionWrapper e) {
            throw e.getCause();
        }
    }

    public static FastaIndex createFromEntries(SortedSet<Entry> entries) {
        return new FastaIndex(entries);
    }

    public static FastaIndex read(File file) throws IOException {
        try (Reader in = new FileReader(file)) {
            return read(in);
        }
    }

    public static FastaIndex read(InputStream in) throws IOException {
        return read(new InputStreamReader(in));
    }

    public static FastaIndex read(Reader reader) throws IOException {
        BufferedReader in = reader instanceof BufferedReader
                ? (BufferedReader) reader
                : new BufferedReader(reader);
        SortedSet<Entry> entries = new TreeSet<>();
        for (String line = in.readLine(); null != line; line = in.readLine()) {
            entries.add(Entry.parse(line));
        }
        return createFromEntries(entries);
    }

    private final SortedSet<Entry> entries;

    private FastaIndex(SortedSet<Entry> entries) {
        this.entries = entries;
    }

    public SortedSet<Entry> entries() {
        return entries;
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj
                || null != obj
                && FastaIndex.class == obj.getClass()
                && Objects.equals(entries(), ((FastaIndex) obj).entries());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(entries());
    }

    @Override
    public String toString() {
        return Joiner
                .on(String.format("%n"))
                .join(Iterables.transform(entries(), Functions.toStringFunction()));
    }

    public FastaIndex write(File file) throws IOException {
        try (Writer out = new FileWriter(file)) {
            return write(out);
        }
    }

    public FastaIndex write(OutputStream out) throws IOException {
        return write(new OutputStreamWriter(out));
    }

    public FastaIndex write(Writer writer) throws IOException {
        PrintWriter out = writer instanceof PrintWriter
                ? (PrintWriter) writer
                : new PrintWriter(writer);
        out.println(this);
        if (out.checkError()) {
            throw new IOException();
        }
        return this;
    }
}
