package org.broadinstitute.hellbender.utils.read;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.reference.IndexedFastaSequenceFile;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.broadinstitute.hellbender.utils.BaseUtils;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.fasta.CachingIndexedFastaSequenceFile;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;


public class ReadUtilsUnitTest extends BaseTest {
    private interface GetAdaptorFunc {
        public int getAdaptor(final MutableRead record);
    }

    @DataProvider(name = "AdaptorGetter")
    public Object[][] makeActiveRegionCutTests() {
        final List<Object[]> tests = new LinkedList<>();

        tests.add( new Object[]{ new GetAdaptorFunc() {
            @Override public int getAdaptor(final MutableRead record) { return ReadUtils.getAdaptorBoundary(record); }
        }});

        tests.add( new Object[]{ new GetAdaptorFunc() {
            @Override public int getAdaptor(final MutableRead record) { return ReadUtils.getAdaptorBoundary(record); }
        }});

        return tests.toArray(new Object[][]{});
    }

    private MutableRead makeRead(final int fragmentSize, final int mateStart) {
        final byte[] bases = {'A', 'C', 'G', 'T', 'A', 'C', 'G', 'T'};
        final byte[] quals = {30, 30, 30, 30, 30, 30, 30, 30};
        final String cigar = "8M";
        MutableRead read = ArtificialReadUtils.createArtificialRead(bases, quals, cigar);
        read.setIsProperPaired(true);
        read.setIsPaired(true);
        read.setMatePosition(read.getContig(), mateStart);
        read.setFragmentLength(fragmentSize);
        return read;
    }

    @Test(dataProvider = "AdaptorGetter")
    public void testGetAdaptorBoundary(final GetAdaptorFunc get) {
        final int fragmentSize = 10;
        final int mateStart = 1000;
        final int BEFORE = mateStart - 2;
        final int AFTER = mateStart + 2;
        int myStart, boundary;
        MutableRead read;

        // Test case 1: positive strand, first read
        read = makeRead(fragmentSize, mateStart);
        myStart = BEFORE;
        read.setPosition(read.getContig(), myStart);
        read.setIsNegativeStrand(false);
        read.setMateIsNegativeStrand(true);
        boundary = get.getAdaptor(read);
        Assert.assertEquals(boundary, myStart + fragmentSize + 1);

        // Test case 2: positive strand, second read
        read = makeRead(fragmentSize, mateStart);
        myStart = AFTER;
        read.setPosition(read.getContig(), myStart);
        read.setIsNegativeStrand(false);
        read.setMateIsNegativeStrand(true);
        boundary = get.getAdaptor(read);
        Assert.assertEquals(boundary, myStart + fragmentSize + 1);

        // Test case 3: negative strand, second read
        read = makeRead(fragmentSize, mateStart);
        myStart = AFTER;
        read.setPosition(read.getContig(), myStart);
        read.setIsNegativeStrand(true);
        read.setMateIsNegativeStrand(false);
        boundary = get.getAdaptor(read);
        Assert.assertEquals(boundary, mateStart - 1);

        // Test case 4: negative strand, first read
        read = makeRead(fragmentSize, mateStart);
        myStart = BEFORE;
        read.setPosition(read.getContig(), myStart);
        read.setIsNegativeStrand(true);
        read.setMateIsNegativeStrand(false);
        boundary = get.getAdaptor(read);
        Assert.assertEquals(boundary, mateStart - 1);

        // Test case 5: mate is mapped to another chromosome (test both strands)
        read = makeRead(fragmentSize, mateStart);
        read.setFragmentLength(0);
        read.setIsNegativeStrand(true);
        read.setMateIsNegativeStrand(false);
        boundary = get.getAdaptor(read);
        Assert.assertEquals(boundary, ReadUtils.CANNOT_COMPUTE_ADAPTOR_BOUNDARY);
        read.setIsNegativeStrand(false);
        read.setMateIsNegativeStrand(true);
        boundary = get.getAdaptor(read);
        Assert.assertEquals(boundary, ReadUtils.CANNOT_COMPUTE_ADAPTOR_BOUNDARY);
        read.setFragmentLength(10);

        // Test case 6: read is unmapped
        read = makeRead(fragmentSize, mateStart);
        read.setIsUnmapped();
        boundary = get.getAdaptor(read);
        Assert.assertEquals(boundary, ReadUtils.CANNOT_COMPUTE_ADAPTOR_BOUNDARY);
        read.setPosition(read.getContig(), myStart);

        // Test case 7:  reads don't overlap and look like this:
        //    <--------|
        //                 |------>
        // first read:
        read = makeRead(fragmentSize, mateStart);
        myStart = 980;
        read.setPosition(read.getContig(), myStart);
        read.setFragmentLength(20);
        read.setIsNegativeStrand(true);
        boundary = get.getAdaptor(read);
        Assert.assertEquals(boundary, ReadUtils.CANNOT_COMPUTE_ADAPTOR_BOUNDARY);

        // second read:
        read = makeRead(fragmentSize, mateStart);
        myStart = 1000;
        read.setPosition(read.getContig(), myStart);
        read.setFragmentLength(20);
        read.setMatePosition(read.getContig(), 980);
        read.setIsNegativeStrand(false);
        boundary = get.getAdaptor(read);
        Assert.assertEquals(boundary, ReadUtils.CANNOT_COMPUTE_ADAPTOR_BOUNDARY);

        // Test case 8: read doesn't have proper pair flag set
        read = makeRead(fragmentSize, mateStart);
        read.setIsPaired(true);
        read.setIsProperPaired(false);
        Assert.assertEquals(get.getAdaptor(read), ReadUtils.CANNOT_COMPUTE_ADAPTOR_BOUNDARY);

        // Test case 9: read and mate have same negative flag setting
        for ( final boolean negFlag: Arrays.asList(true, false) ) {
            read = makeRead(fragmentSize, mateStart);
            read.setPosition(read.getContig(), BEFORE);
            read.setIsPaired(true);
            read.setIsProperPaired(true);
            read.setIsNegativeStrand(negFlag);
            read.setMateIsNegativeStrand(!negFlag);
            Assert.assertTrue(get.getAdaptor(read) != ReadUtils.CANNOT_COMPUTE_ADAPTOR_BOUNDARY, "Get adaptor should have succeeded");

            read = makeRead(fragmentSize, mateStart);
            read.setPosition(read.getContig(), BEFORE);
            read.setIsPaired(true);
            read.setIsProperPaired(true);
            read.setIsNegativeStrand(negFlag);
            read.setMateIsNegativeStrand(negFlag);
            Assert.assertEquals(get.getAdaptor(read), ReadUtils.CANNOT_COMPUTE_ADAPTOR_BOUNDARY, "Get adaptor should have failed for reads with bad alignment orientation");
        }
    }

    @Test
    public void testGetBasesReverseComplement() {
        int iterations = 1000;
        Random random = Utils.getRandomGenerator();
        while(iterations-- > 0) {
            final int l = random.nextInt(1000);
            MutableRead read = ArtificialReadUtils.createRandomRead(l);
            byte [] original = read.getBases();
            byte [] reconverted = new byte[l];
            String revComp = ReadUtils.getBasesReverseComplement(read);
            for (int i=0; i<l; i++) {
                reconverted[l-1-i] = BaseUtils.getComplement((byte) revComp.charAt(i));
            }
            Assert.assertEquals(reconverted, original);
        }
    }

    @Test
    public void testGetMaxReadLength() {
        for( final int minLength : Arrays.asList( 5, 30, 50 ) ) {
            for( final int maxLength : Arrays.asList( 50, 75, 100 ) ) {
                final List<MutableRead> reads = new ArrayList<>();
                for( int readLength = minLength; readLength <= maxLength; readLength++ ) {
                    reads.add( ArtificialReadUtils.createRandomRead( readLength ) );
                }
                Assert.assertEquals(ReadUtils.getMaxReadLength(reads), maxLength, "max length does not match");
            }
        }

        final List<MutableRead> reads = new LinkedList<>();
        Assert.assertEquals(ReadUtils.getMaxReadLength(reads), 0, "Empty list should have max length of zero");
    }

    @Test
    public void testReadWithNsRefIndexInDeletion() throws FileNotFoundException {

        final IndexedFastaSequenceFile seq = new CachingIndexedFastaSequenceFile(new File(exampleReference));
        final SAMFileHeader header = ArtificialReadUtils.createArtificialSamHeader(seq.getSequenceDictionary());
        final int readLength = 76;

        final MutableRead read = ArtificialReadUtils.createArtificialRead(header, "myRead", 0, 8975, readLength);
        read.setBases(Utils.dupBytes((byte) 'A', readLength));
        read.setBaseQualities(Utils.dupBytes((byte)30, readLength));
        read.setCigar("3M414N1D73M");

        final int result = ReadUtils.getReadCoordinateForReferenceCoordinateUpToEndOfRead(read, 9392, ReadUtils.ClippingTail.LEFT_TAIL);
        Assert.assertEquals(result, 2);
    }

    @Test
    public void testReadWithNsRefAfterDeletion() throws FileNotFoundException {

        final IndexedFastaSequenceFile seq = new CachingIndexedFastaSequenceFile(new File(exampleReference));
        final SAMFileHeader header = ArtificialReadUtils.createArtificialSamHeader(seq.getSequenceDictionary());
        final int readLength = 76;

        final MutableRead read = ArtificialReadUtils.createArtificialRead(header, "myRead", 0, 8975, readLength);
        read.setBases(Utils.dupBytes((byte) 'A', readLength));
        read.setBaseQualities(Utils.dupBytes((byte)30, readLength));
        read.setCigar("3M414N1D73M");

        final int result = ReadUtils.getReadCoordinateForReferenceCoordinateUpToEndOfRead(read, 9393, ReadUtils.ClippingTail.LEFT_TAIL);
        Assert.assertEquals(result, 3);
    }

    @DataProvider(name = "HasWellDefinedFragmentSizeData")
    public Object[][] makeHasWellDefinedFragmentSizeData() throws Exception {
        final List<Object[]> tests = new LinkedList<Object[]>();

        // setup a basic read that will work
        final SAMFileHeader header = ArtificialReadUtils.createArtificialSamHeader();
        final MutableRead read = ArtificialReadUtils.createArtificialRead(header, "read1", 0, 10, 10);
        read.setIsPaired(true);
        read.setIsProperPaired(true);
        read.setPosition(read.getContig(), 100);
        read.setCigar("50M");
        read.setMatePosition(read.getContig(), 130);
        read.setFragmentLength(80);
        read.setIsFirstOfPair(true);
        read.setIsNegativeStrand(false);
        read.setMateIsNegativeStrand(true);

        tests.add( new Object[]{ "basic case", (MutableRead)read.copy(), true });

        {
            final MutableRead bad1 = (MutableRead)read.copy();
            bad1.setIsPaired(false);
            tests.add( new Object[]{ "not paired", bad1, false });
        }

        {
            final MutableRead bad = (MutableRead)read.copy();
            bad.setIsProperPaired(false);
            // we currently don't require the proper pair flag to be set
            tests.add( new Object[]{ "not proper pair", bad, true });
//            tests.add( new Object[]{ "not proper pair", bad, false });
        }

        {
            final MutableRead bad = (MutableRead)read.copy();
            bad.setIsUnmapped();
            tests.add( new Object[]{ "read is unmapped", bad, false });
        }

        {
            final MutableRead bad = (MutableRead)read.copy();
            bad.setMateIsUnmapped();
            tests.add( new Object[]{ "mate is unmapped", bad, false });
        }

        {
            final MutableRead bad = (MutableRead)read.copy();
            bad.setMateIsNegativeStrand(false);
            tests.add( new Object[]{ "read and mate both on positive strand", bad, false });
        }

        {
            final MutableRead bad = (MutableRead)read.copy();
            bad.setIsNegativeStrand(true);
            tests.add( new Object[]{ "read and mate both on negative strand", bad, false });
        }

        {
            final MutableRead bad = (MutableRead)read.copy();
            bad.setFragmentLength(0);
            tests.add( new Object[]{ "insert size is 0", bad, false });
        }

        {
            final MutableRead bad = (MutableRead)read.copy();
            bad.setPosition(bad.getContig(), 1000);
            tests.add( new Object[]{ "positve read starts after mate end", bad, false });
        }

        {
            final MutableRead bad = (MutableRead)read.copy();
            bad.setIsNegativeStrand(true);
            bad.setMateIsNegativeStrand(false);
            bad.setMatePosition(bad.getMateContig(), 1000);
            tests.add( new Object[]{ "negative strand read ends before mate starts", bad, false });
        }

        return tests.toArray(new Object[][]{});
    }

    @Test(dataProvider = "HasWellDefinedFragmentSizeData")
    private void testHasWellDefinedFragmentSize(final String name, final MutableRead read, final boolean expected) {
        Assert.assertEquals(ReadUtils.hasWellDefinedFragmentSize(read), expected);
    }
}
