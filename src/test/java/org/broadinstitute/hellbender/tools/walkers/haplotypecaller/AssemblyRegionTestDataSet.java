package org.broadinstitute.hellbender.tools.walkers.haplotypecaller;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.broadinstitute.hellbender.tools.walkers.haplotypecaller.readthreading.ReadThreadingGraph;
import org.broadinstitute.hellbender.utils.GenomeLocParser;
import org.broadinstitute.hellbender.utils.QualityUtils;
import org.broadinstitute.hellbender.utils.haplotype.Haplotype;
import org.broadinstitute.hellbender.utils.read.ArtificialSAMUtils;
import org.broadinstitute.hellbender.utils.read.ReadUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
* Mock-up active region data used in testing.
*
* @author Valentin Ruano-Rubio &lt;valentin@broadinstitute.org&gt;
*/
public class AssemblyRegionTestDataSet {

    private final byte[] referenceBytes;
    protected String reference;
    protected String[] haplotypeCigars;
    protected List<String> haplotypeStrings;
    protected String[] readCigars;
    protected byte[] bq;
    protected byte[] dq;
    protected byte[] iq;
    protected int kmerSize;
    private List<Haplotype> haplotypeList;
    private List<SAMRecord> readList;
    private AssemblyResultSet assemblyResultSet;
    private Map<String,SAMRecord> readBySequence;
    private String stringRepresentation;
    private List<List<Civar.ElementOffset>> readEventOffsetList;
    private GenomeLocParser genomeLocParser;

    /** Create a new active region data test set */
    public AssemblyRegionTestDataSet(final int kmerSize, final String reference, final String[] haplotypes,
                                   final String[] readCigars, final byte[] bq, final byte[] dq, final byte[] iq) {
        this.reference = reference;
        this.referenceBytes = reference.getBytes();
        this.haplotypeCigars = haplotypes;
        this.readCigars = readCigars;
        this.bq = bq;
        this.dq = dq;
        this.iq = iq;
        this.kmerSize = kmerSize;
        this.genomeLocParser = new GenomeLocParser(ArtificialSAMUtils.createArtificialSamHeader(1, 1, reference.length()).getSequenceDictionary());
    }

    public String getReference() {
        return reference;
    }

    public String toString() {
        if (stringRepresentation == null)
            return super.toString();
        else return stringRepresentation;
    }

    public AssemblyResultSet assemblyResultSet() {
        if (assemblyResultSet == null) {
            final ReadThreadingGraph rtg = new ReadThreadingGraph(kmerSize);
            rtg.addSequence("anonymous", this.getReference().getBytes(), true);
            for (final String haplotype : this.haplotypesStrings()) {
                rtg.addSequence("anonymous", haplotype.getBytes(), false);
            }
            rtg.buildGraphIfNecessary();
            if (rtg.hasCycles())
                throw new RuntimeException("there is cycles in the reference with kmer size " + kmerSize + ". Don't use this size for the benchmark or change the reference");

            List<Haplotype> haplotypeList = this.haplotypeList();

            assemblyResultSet = new AssemblyResultSet();
            final AssemblyResult ar = new AssemblyResult((haplotypeList.size() > 1 ?
                    AssemblyResult.Status.ASSEMBLED_SOME_VARIATION : AssemblyResult.Status.JUST_ASSEMBLED_REFERENCE),rtg.convertToSequenceGraph());
            ar.setThreadingGraph(rtg);

            for (final Haplotype h : haplotypeList)
                assemblyResultSet.add(h, ar);
        }
        return assemblyResultSet;
    }

    public List<String> haplotypesStrings() {
        if (haplotypeStrings != null) {
            return haplotypeStrings;
        }
        final List<String> result = new ArrayList<>(haplotypeCigars.length);
        String reference = this.reference;
        for (final String cigar : haplotypeCigars) {
            if (cigar.matches("^Civar:.*$")) {
                stringRepresentation = cigar.substring(6);
                result.addAll(expandAllCombinations(cigar.substring(6),reference));
            } else if (cigar.matches("^.*\\d+.*$")) {
                result.add(applyCigar(reference, cigar,0,true));
            } else {
                result.add(cigar);
            }
        }
        haplotypeStrings = result;
        return result;
    }

    private List<String> expandAllCombinations(final String cigarString, final String reference) {
        final Civar civar = Civar.fromCharSequence(cigarString);
        final List<Civar> unrolledCivars = civar.optionalizeAll().unroll();
        List<String> result = new ArrayList<>(unrolledCivars.size());
        for (final Civar c : unrolledCivars) {
            result.add(c.applyTo(reference));
        }
        return result;
    }

    private List<Haplotype> expandAllHaplotypeCombinations(final String civarString, final String reference) {
        final Civar civar = Civar.fromCharSequence(civarString);
        final List<Civar> unrolledCivars = civar.optionalizeAll().unroll();
        List<Haplotype> result = new ArrayList<>(unrolledCivars.size());
        for (final Civar c : unrolledCivars) {
            final String baseString = c.applyTo(reference);
            final Haplotype haplotype = new Haplotype(baseString.getBytes(),baseString.equals(reference));
            haplotype.setGenomeLocation(genomeLocParser.createGenomeLoc("chr1",1,reference.length()));
            try {
            haplotype.setCigar(c.toCigar(reference.length()));
            } catch (final RuntimeException ex) {
                c.applyTo(reference);
                c.toCigar(reference.length());
                throw new RuntimeException("" + c + " " + ex.getMessage(),ex);
            }
            result.add(haplotype);
        }
        return result;
    }


    public List<Haplotype> haplotypeList() {
        if (haplotypeList == null) {

          final List<Haplotype> result = new ArrayList<>(haplotypeCigars.length);
          final String reference = this.reference;
          for (final String cigar : haplotypeCigars) {
              if (cigar.matches("^Civar:.*$")) {
                  stringRepresentation = cigar.substring(6);
                  result.addAll(expandAllHaplotypeCombinations(cigar.substring(6), reference));
              } else if (cigar.matches("^.*\\d+.*$")) {
                  result.add(cigarToHaplotype(reference, cigar, 0, true));
              } else {
                  final Haplotype h = new Haplotype(cigar.getBytes());
                  h.setGenomeLocation(genomeLocParser.createGenomeLoc("chr1",1,reference.length()));
                  result.add(h);
              }
          }
          haplotypeList = result;
        }
        return haplotypeList;
    }


    protected SAMSequenceDictionary artificialSAMSequenceDictionary() {
        return new SAMSequenceDictionary(Collections.singletonList(new SAMSequenceRecord("00", reference.length())));
    }

    protected SAMFileHeader artificialSAMFileHeader() {
        return ArtificialSAMUtils.createArtificialSamHeader(artificialSAMSequenceDictionary());
    }

    public List<SAMRecord> readList() {
        if (readList == null) {
            final SAMFileHeader header = artificialSAMFileHeader();
            readList = new ArrayList<>(readCigars.length);
            final List<String> haplotypes = haplotypesStrings();
            int count = 0;
            for (final String descr : readCigars) {
                String sequence;
                if (descr.matches("^\\d+:\\d+:.+$")) {
                    final String[] parts = descr.split(":");
                    int allele = Integer.valueOf(parts[0]);
                    int offset = Integer.valueOf(parts[1]);
                    final String cigar = parts[2];
                    final String base = allele == 0 ? reference : haplotypes.get(allele - 1);
                    sequence = applyCigar(base, cigar, offset, false);
                    final SAMRecord samRecord = ArtificialSAMUtils.createArtificialRead(header, "read_" + count, 0, 1, sequence.getBytes(), Arrays.copyOf(bq, sequence.length()));
                    readList.add(new MySAMRecord(samRecord));
                } else if (descr.matches("^\\*:\\d+:\\d+$")) {
                    int readCount = Integer.valueOf(descr.split(":")[1]);
                    int readLength = Integer.valueOf(descr.split(":")[2]);
                    readList.addAll(generateSamRecords(haplotypes, readCount, readLength, header, count));
                } else {
                    sequence = descr;
                    final SAMRecord samRecord = ArtificialSAMUtils.createArtificialRead(header, "read_" + count, 0, 1, sequence.getBytes(), Arrays.copyOf(bq, sequence.length()));
                    readList.add(new MySAMRecord(samRecord));
                }
                count = readList.size();
            }
        }
        return readList;
    }

    public List<List<Civar.ElementOffset>> readEventOffsetList() {
        if (haplotypeCigars.length != 1 || !haplotypeCigars[0].startsWith("Civar:"))
            throw new UnsupportedOperationException();
        if (readEventOffsetList == null) {
            final Civar civar = Civar.fromCharSequence(haplotypeCigars[0].substring(6));
            final List<Civar> unrolledCivars = civar.optionalizeAll().unroll();

            readEventOffsetList = new ArrayList<>(readCigars.length);
            int count = 0;
            for (final String descr : readCigars) {
                if (descr.matches("^\\d+:\\d+:.+$")) {
                    throw new UnsupportedOperationException();
                } else if (descr.matches("^\\*:\\d+:\\d+$")) {
                    int readCount = Integer.valueOf(descr.split(":")[1]);
                    int readLength = Integer.valueOf(descr.split(":")[2]);
                    readEventOffsetList.addAll(generateElementOffsetRecords(haplotypesStrings(), unrolledCivars, readCount, readLength, count));
                } else {
                    throw new UnsupportedOperationException();
                }
                count = readEventOffsetList.size();
            }
            readEventOffsetList = Collections.unmodifiableList(readEventOffsetList);
        }
        return readEventOffsetList;
    }

    public String cigarToSequence(final String cigar) {
            String reference = this.reference;
            return applyCigar(reference, cigar,0,true);
    }

    public SAMRecord readFromString(final String readSequence) {
        if (readBySequence == null) {
            final List<SAMRecord> readList = readList();
            readBySequence = new HashMap<>(readList.size());
            for (final SAMRecord r : readList)
                readBySequence.put(r.getReadString(),r);
        }
        return readBySequence.get(readSequence);
    }

    public List<Civar> unrolledCivars() {
        if (haplotypeCigars.length != 1 || !haplotypeCigars[0].startsWith("Civar:"))
            throw new UnsupportedOperationException();
        final Civar civar = Civar.fromCharSequence(haplotypeCigars[0].substring(6));
        return civar.optionalizeAll().unroll();
    }

    public void introduceErrors(final Random rnd) {
        final List<SAMRecord> reads = readList();
        final ArrayList<SAMRecord> result = new ArrayList<>(reads.size());
        for (final SAMRecord read : reads) {
            result.add(new MySAMRecord(read,rnd));
        }
        readList = result;
    }

    private class MySAMRecord extends SAMRecord {
            protected MySAMRecord(final SAMRecord r) {
                super(r);
                this.setMappingQuality(100);
                GATKBin.setReadIndexingBin(this, -1);
            }

        ExponentialDistribution indelLengthDist = MathUtils.exponentialDistribution(1.0 / 0.9);

        public MySAMRecord(final SAMRecord r, final Random rnd) {
            super(r);
            this.setMappingQuality(100);
            // setting read indexing bin last

            final byte[] bases = new byte[r.getReadBases().length];

            final byte[] readBases = r.getReadBases();
            final byte[] bq = r.getBaseQualities();
            final byte[] iq = ReadUtils.getBaseInsertionQualities(r);
            final byte[] dq = ReadUtils.getBaseDeletionQualities(r);
            int refOffset = r.getAlignmentStart() - 1;
            int readOffset = 0;
            for (int i = 0; i < r.getReadBases().length;) {
                double p = rnd.nextDouble();
                double iqp = QualityUtils.qualToErrorProb(iq[i]);
                if (p < iqp) { // insertion
                    final int length = Math.min(generateIndelLength(rnd), r.getReadBases().length - i);
                    final int refStart = rnd.nextInt(reference.length() - length);
                    System.arraycopy(referenceBytes, refStart, bases, i, length);
                    i += length;
                    continue;
                }
                p -= iqp;
                double dqp = QualityUtils.qualToErrorProb(dq[i]);
                if (p < dqp) {
                    final int length = generateIndelLength(rnd);
                    refOffset += length;
                    refOffset = refOffset % referenceBytes.length;
                    readOffset += length;
                    continue;
                }
                p -= dqp;
                double bqp = QualityUtils.qualToErrorProb(bq[i]);
                byte b = readOffset < readBases.length ? readBases[readOffset] : referenceBytes[refOffset];
                byte nb;
                if (p < bqp) {
                   switch (b) {
                       case 'A': nb = 'C'; break;
                       case 'T': nb = 'A'; break;
                       case 'C': nb = 'G'; break;
                       case 'G': nb = 'B'; break;
                       default: nb = 'A';
                   }
                } else
                    nb = b;

                bases[i++] = nb;
                refOffset++;
                refOffset = refOffset % referenceBytes.length;
                readOffset++;
            }
            this.setReadBases(bases);
            this.setBaseQualities(r.getBaseQualities());
            this.setReadName(r.getReadName());

            GATKBin.setReadIndexingBin(this, -1);
        }

        private int generateIndelLength(final Random rnd) {
            final int length;
            try {
                length = (int) Math.round(indelLengthDist.inverseCumulativeProbability(rnd.nextDouble()) + 1);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return length;
        }

        @Override
            public byte[] getBaseDeletionQualities() {
                return Arrays.copyOf(dq, getReadLength());
            }

            @Override
            public byte[] getBaseInsertionQualities() {
                return Arrays.copyOf(iq, getReadLength());
            }

            @Override
            public int getMappingQuality() {
                return 100;
            }

            @Override
            public int hashCode() {
                return getReadName().hashCode();
            }

            @Override
            public boolean equals(Object o) {
                if (o instanceof SAMRecord) {
                    return getReadName().equals(((SAMRecord)o).getReadName());
                } else {
                    return false;
                }
            }

            public String toString() {
                return super.toString() + " " + this.getReadString();
            }
    }


    public List<String> readStrings() {
        final List<String> result = new ArrayList<>(readCigars.length);
        final List<String> haplotypes = haplotypesStrings();
        for (final String descr : readCigars) {
            String sequence;
            if (descr.matches("^\\d+:\\d+:.+$")) {
                final String[] parts = descr.split(":");
                int allele = Integer.valueOf(parts[0]);
                int offset = Integer.valueOf(parts[1]);
                final String cigar = parts[2];
                final String base = allele == 0 ? reference : haplotypes.get(allele - 1);
                sequence = applyCigar(base, cigar, offset, false);
                result.add(sequence);
            } else if (descr.matches("\\*:^\\d+:\\d+")) {
                int readCount = Integer.valueOf(descr.split(":")[1]);
                int readLength = Integer.valueOf(descr.split(":")[2]);
                result.addAll(generateReads(haplotypes, readCount, readLength));
            } else {
                sequence = descr;
                result.add(sequence);
            }
        }
        return result;
    }

    private List<String> generateReads(final List<String> haplotypes, final int readCount, final int readLength) {
        final List<String> result = new ArrayList<>(readCount);
        for (int i = 0; i < readCount; i++) {
            int hi = i % haplotypes.size();
            final String h = haplotypes.get(hi);
            int offset = i % h.length() - readLength;
            result.add(h.substring(offset,offset + readLength));
        }
        return result;
    }

    private List<MySAMRecord> generateSamRecords(final List<String> haplotypes, final int readCount, final int readLength, final SAMFileHeader header, final int idStart) {
        int id = idStart;
        final List<MySAMRecord> result = new ArrayList<>(readCount);
        for (int i = 0; i < readCount; i++) {
            int hi = i % haplotypes.size();
            final String h = haplotypes.get(hi);
            int offset = h.length() <= readLength ? 0 : i % (h.length() - readLength);
            int to = Math.min(h.length(), offset + readLength);
            byte[] bases = h.substring(offset,to).getBytes();
            byte[] quals = Arrays.copyOf(bq, to - offset);
            final SAMRecord samRecord = ArtificialSAMUtils.createArtificialRead(header,"read_" + id++,0,offset + 1,bases, quals);
            result.add(new MySAMRecord(samRecord));
        }
        return result;
    }


    private List<List<Civar.ElementOffset>> generateElementOffsetRecords(final List<String> haplotypes, final List<Civar> unrolledCivars, final int readCount, final int readLength, final int count) {

        final List<List<Civar.ElementOffset>> result = new ArrayList<>(readCount);
        for (int i = 0; i < readCount; i++) {
            int hi = i % unrolledCivars.size();
            final Civar c = unrolledCivars.get(hi);
            final String h = haplotypes.get(hi);
            int offset = h.length() <= readLength ? 0 : i % (h.length() - readLength);
            int to = Math.min(h.length(), offset + readLength);
            result.add(c.eventOffsets(reference,offset,to));
        }
        return result;
    }

    private static final Pattern cigarPattern = Pattern.compile("(\\d+)([=A-Z])");


    private Haplotype cigarToHaplotype(final String reference, final String cigar, final int offset, final boolean global) {
        final String sequence = applyCigar(reference,cigar,offset,global);
        final Haplotype haplotype = new Haplotype(sequence.getBytes(),reference.equals(sequence));
        haplotype.setGenomeLocation(genomeLocParser.createGenomeLoc("chr1",1,reference.length()));
        haplotype.setCigar(Civar.fromCharSequence(cigar).toCigar(reference.length()));
        return haplotype;
    }

    private String applyCigar(final String reference, final String cigar, final int offset, final boolean global) {
        final Matcher pm = cigarPattern.matcher(cigar);
        StringBuffer sb = new StringBuffer();
        int index = offset;
        while (pm.find()) {
            int length = Integer.valueOf(pm.group(1));
            char operator = pm.group(2).charAt(0);
            switch (operator) {
                case '=' :
                    try {
                      sb.append(reference.substring(index, index + length));
                    } catch (Exception e) {
                      throw new RuntimeException(" " + index + " " + (index + length) + " " + reference.length() + " " + cigar,e);
                    }
                    index += length; break;
                case 'D' :
                    index += length; break;
                case 'I' :
                    String insert = cigar.substring(pm.end(),pm.end() + length).toUpperCase();
                    sb.append(insert); break;
                case 'V' :
                    sb.append(transversionV(reference.charAt(index))); index++; break;
                case 'W' :
                        sb.append(transversionW(reference.charAt(index))); index++; break;
                case 'T' :
                    sb.append(transition(reference.charAt(index))); index++; break;
                default:
                    throw new UnsupportedOperationException("cigar operator " + operator + " not supported.");
            }
        }
        if (global && index != reference.length()) {
            throw new RuntimeException(" haplotype cigar does not explain reference length (" + index + " != " + reference.length() + ") on cigar " + cigar);
        } else if (index > reference.length()) {
            throw new RuntimeException(" index beyond end ");
        }
        return sb.toString();
    }

    protected int kmerSize() {
        return kmerSize;
    }

    private char transversionV(final char c) {
        switch (Character.toUpperCase(c)) {
            case 'A': return 'C';
            case 'G': return 'T';
            case 'C': return 'A';
            case 'T': return 'G';
            default:
                return c;
        }

    }

    private char transversionW(final char c) {
        switch (Character.toUpperCase(c)) {
            case 'A': return 'T';
            case 'G': return 'C';
            case 'T': return 'A';
            case 'C': return 'G';
            default:
                return c;
        }

    }

    private char transition(final char c) {
        switch (Character.toUpperCase(c)) {
            case 'A': return 'G';
            case 'G': return 'A';
            case 'T': return 'C';
            case 'C': return 'T';
            default:
                return c;
        }

    }
}
