package org.broadinstitute.hellbender.utils.read;

import htsjdk.samtools.*;
import htsjdk.samtools.util.StringUtil;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.BaseUtils;
import org.broadinstitute.hellbender.utils.NGSPlatform;
import org.broadinstitute.hellbender.utils.recalibration.EventType;

import java.util.*;

/**
 * A miscellaneous collection of utilities for working with reads, headers, etc.
 * Static methods only, please.
 */
public class ReadUtils {
    private final static Logger logger = LogManager.getLogger(ReadUtils.class);

    private ReadUtils() {
    }

    /**
     * The default quality score for an insertion or deletion, if
     * none are provided for this read.
     */
    public static final byte DEFAULT_INSERTION_DELETION_QUAL = (byte) 45;

    // Base Quality Score Recalibrator specific attribute tags
    public static final String BQSR_BASE_INSERTION_QUALITIES = "BI";                // base qualities for insertions
    public static final String BQSR_BASE_DELETION_QUALITIES = "BD";                 // base qualities for deletions

    public static final int CLIPPING_GOAL_NOT_REACHED = -1;

    public static final String NULL_SEQUENCE_STRING = "*";
    public static final String ORIGINAL_BASE_QUALITIES_TAG = "OQ";


    /**
     * HACK: This is used to make a copy of a read.
     * Really, SAMRecord should provide a copy constructor or a factory method.
     */
    public static SAMRecord cloneSAMRecord( SAMRecord originalRead ) {
        if ( originalRead == null ) return null;
        try {
            return (SAMRecord) originalRead.clone();
        }
        catch ( CloneNotSupportedException e ) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * HACK: This is used to make a copy of a header.
     * Really, SAMFileHeader should provide a copy constructor or a factory method.
     */
    public static SAMFileHeader cloneSAMFileHeader( SAMFileHeader header ) {
        if (header == null) return null;
        return header.clone();
    }

    public static boolean isEmpty(Read read) {
        return read.getBases() == null || read.getLength() == 0;
    }

    public static String getBasesString( final Read read ) {
        final byte[] readBases = read.getBases();
        if (readBases.length == 0) {
            return NULL_SEQUENCE_STRING;
        }
        return StringUtil.bytesToString(readBases);
    }

    /**
     * Convert bases in place into canonical form, upper case, and with no-call represented as N.
     *
     * @param bases bases to normalize
     */
    public static void normalizeBases( final byte[] bases ) {
        for ( int i = 0; i < bases.length; ++i ) {
            bases[i] = StringUtil.toUpperCase(bases[i]);
            if (bases[i] == '.') {
                bases[i] = 'N';
            }
        }
    }

    public static byte[] getOriginalBaseQualities( final Read read ) {
        if ( ! read.hasAttribute(ORIGINAL_BASE_QUALITIES_TAG) ) {
            return null;
        }
        final String oqString = read.getAttributeAsString(ORIGINAL_BASE_QUALITIES_TAG);
        return oqString.length() > 0 ? SAMUtils.fastqToPhred(oqString) : null;
    }

    public static int getReferenceIndex( final Read read, final SAMFileHeader header ) {
        if ( read.isUnmapped() ) {
            return SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;
        }

        final int referenceIndex = header.getSequenceIndex(read.getContig());
        if ( referenceIndex == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX ) {
            throw new IllegalArgumentException(String.format("Contig %s not found in provided header", read.getContig()));
        }
        return referenceIndex;
    }

    public static int getMateReferenceIndex( final Read read, final SAMFileHeader header ) {
        if ( read.mateIsUnmapped() ) {
            return SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;
        }

        final int mateReferenceIndex = header.getSequenceIndex(read.getMateContig());
        if ( mateReferenceIndex == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX ) {
            throw new IllegalArgumentException(String.format("Contig %s not found in provided header", read.getMateContig()));
        }
        return mateReferenceIndex;
    }

    public static SAMReadGroupRecord getSAMReadGroupRecordForRead( final Read read, final SAMFileHeader header ) {
        return header.getReadGroup(read.getReadGroup());
    }

    public static String getPlatformForRead( final Read read, final SAMFileHeader header ) {
        final SAMReadGroupRecord readGroup = getSAMReadGroupRecordForRead(read, header);
        return readGroup.getPlatform();
    }

    public static String getSampleNameForRead( final Read read, final SAMFileHeader header ) {
        final SAMReadGroupRecord readGroup = getSAMReadGroupRecordForRead(read, header);
        return readGroup.getSample();
    }

    public static String prettyPrintSequenceRecords ( SAMSequenceDictionary sequenceDictionary ) {
        String[] sequenceRecordNames = new String[sequenceDictionary.size()];
        int sequenceRecordIndex = 0;
        for (SAMSequenceRecord sequenceRecord : sequenceDictionary.getSequences()) {
            sequenceRecordNames[sequenceRecordIndex++] = sequenceRecord.getSequenceName();
        }
        return Arrays.deepToString(sequenceRecordNames);
    }

    /**
     * A marker to tell which end of the read has been clipped
     */
    public enum ClippingTail {
        LEFT_TAIL,
        RIGHT_TAIL
    }

    public static final int SAM_READ_PAIRED_FLAG = 0x1;
    public static final int SAM_PROPER_PAIR_FLAG = 0x2;
    public static final int SAM_READ_UNMAPPED_FLAG = 0x4;
    public static final int SAM_MATE_UNMAPPED_FLAG = 0x8;
    public static final int SAM_READ_STRAND_FLAG = 0x10;
    public static final int SAM_MATE_STRAND_FLAG = 0x20;
    public static final int SAM_FIRST_OF_PAIR_FLAG = 0x40;
    public static final int SAM_SECOND_OF_PAIR_FLAG = 0x80;
    public static final int SAM_NOT_PRIMARY_ALIGNMENT_FLAG = 0x100;
    public static final int SAM_READ_FAILS_VENDOR_QUALITY_CHECK_FLAG = 0x200;
    public static final int SAM_DUPLICATE_READ_FLAG = 0x400;
    public static final int SAM_SUPPLEMENTARY_ALIGNMENT_FLAG = 0x800;

    public static int getSAMFlagsForRead( final Read read ) {
        int samFlags = 0;

        if ( read.isPaired() ) {
            samFlags |= SAM_READ_PAIRED_FLAG;
        }
        if ( read.isProperlyPaired() ) {
            samFlags |= SAM_PROPER_PAIR_FLAG;
        }
        if ( read.isUnmapped() ) {
            samFlags |= SAM_READ_UNMAPPED_FLAG;
        }
        if ( read.mateIsUnmapped() ) {
            samFlags |= SAM_MATE_UNMAPPED_FLAG;
        }
        if ( read.isReverseStrand() ) {
            samFlags |= SAM_READ_STRAND_FLAG;
        }
        if ( read.mateIsReverseStrand() ) {
            samFlags |= SAM_MATE_STRAND_FLAG;
        }
        if ( read.isFirstOfPair() ) {
            samFlags |= SAM_FIRST_OF_PAIR_FLAG;
        }
        if ( read.isSecondOfPair() ) {
            samFlags |= SAM_SECOND_OF_PAIR_FLAG;
        }
        if ( read.isNonPrimaryAlignment() ) {
            samFlags |= SAM_NOT_PRIMARY_ALIGNMENT_FLAG;
        }
        if ( read.failsVendorQualityCheck() ) {
            samFlags |= SAM_READ_FAILS_VENDOR_QUALITY_CHECK_FLAG;
        }
        if ( read.isDuplicate() ) {
            samFlags |= SAM_DUPLICATE_READ_FLAG;
        }
        if ( read.isSupplementaryAlignment() ) {
            samFlags |= SAM_SUPPLEMENTARY_ALIGNMENT_FLAG;
        }

        return samFlags;
    }

    /**
     * Finds the adaptor boundary around the read and returns the first base inside the adaptor that is closest to
     * the read boundary. If the read is in the positive strand, this is the first base after the end of the
     * fragment (Picard calls it 'insert'), if the read is in the negative strand, this is the first base before the
     * beginning of the fragment.
     *
     * There are two cases we need to treat here:
     *
     * 1) Our read is in the reverse strand :
     *
     *     <----------------------| *
     *   |--------------------->
     *
     *   in these cases, the adaptor boundary is at the mate start (minus one)
     *
     * 2) Our read is in the forward strand :
     *
     *   |---------------------->   *
     *     <----------------------|
     *
     *   in these cases the adaptor boundary is at the start of the read plus the inferred insert size (plus one)
     *
     * @param read the read being tested for the adaptor boundary
     * @return the reference coordinate for the adaptor boundary (effectively the first base IN the adaptor, closest to the read.
     * CANNOT_COMPUTE_ADAPTOR_BOUNDARY if the read is unmapped or the mate is mapped to another contig.
     */
    public static int getAdaptorBoundary(final Read read) {
        if ( ! hasWellDefinedFragmentSize(read) ) {
            return CANNOT_COMPUTE_ADAPTOR_BOUNDARY;
        } else if ( read.isReverseStrand() ) {
            return read.getMateStart() - 1;           // case 1 (see header)
        } else {
            final int insertSize = Math.abs(read.getFragmentLength());    // the inferred insert size can be negative if the mate is mapped before the read (so we take the absolute value)
            return read.getStart() + insertSize + 1;  // case 2 (see header)
        }
    }

    public static int CANNOT_COMPUTE_ADAPTOR_BOUNDARY = Integer.MIN_VALUE;

    /**
     * Can the adaptor sequence of read be reliably removed from the read based on the alignment of
     * read and its mate?
     *
     * @param read the read to check
     * @return true if it can, false otherwise
     */
    public static boolean hasWellDefinedFragmentSize(final Read read) {
        if ( read.getFragmentLength() == 0 )
            // no adaptors in reads with mates in another chromosome or unmapped pairs
            return false;
        if ( ! read.isPaired() )
            // only reads that are paired can be adaptor trimmed
            return false;
        if ( read.isUnmapped() || read.mateIsUnmapped() )
            // only reads when both reads are mapped can be trimmed
            return false;
//        if ( ! read.isProperlyPaired() )
//            // note this flag isn't always set properly in BAMs, can will stop us from eliminating some proper pairs
//            // reads that aren't part of a proper pair (i.e., have strange alignments) can't be trimmed
//            return false;
        if ( read.isReverseStrand() == read.mateIsReverseStrand() )
            // sanity check on isProperlyPaired to ensure that read1 and read2 aren't on the same strand
            return false;

        if ( read.isReverseStrand() ) {
            // we're on the negative strand, so our read runs right to left
            return read.getEnd() > read.getMateStart();
        } else {
            // we're on the positive strand, so our mate should be to our right (his start + insert size should be past our start)
            return read.getStart() <= read.getMateStart() + read.getFragmentLength();
        }
    }

    /**
     * If a read starts in INSERTION, returns the first element length.
     *
     * Warning: If the read has Hard or Soft clips before the insertion this function will return 0.
     *
     * @param read
     * @return the length of the first insertion, or 0 if there is none (see warning).
     */
    public static int getFirstInsertionOffset(Read read) {
        CigarElement e = read.getCigar().getCigarElement(0);
        if ( e.getOperator() == CigarOperator.I )
            return e.getLength();
        else
            return 0;
    }

    /**
     * If a read ends in INSERTION, returns the last element length.
     *
     * Warning: If the read has Hard or Soft clips after the insertion this function will return 0.
     *
     * @param read
     * @return the length of the last insertion, or 0 if there is none (see warning).
     */
    public static int getLastInsertionOffset(Read read) {
        CigarElement e = read.getCigar().getCigarElement(read.getCigar().numCigarElements() - 1);
        if ( e.getOperator() == CigarOperator.I )
            return e.getLength();
        else
            return 0;
    }

    /**
     * Calculates the reference coordinate for the beginning of the read taking into account soft clips but not hard clips.
     *
     * Note: getUnclippedStart() adds soft and hard clips, this function only adds soft clips.
     *
     * @return the unclipped start of the read taking soft clips (but not hard clips) into account
     */
    public static int getSoftStart(Read read) {
        int softStart = read.getStart();
        for (final CigarElement cig : read.getCigar().getCigarElements()) {
            final CigarOperator op = cig.getOperator();

            if (op == CigarOperator.SOFT_CLIP)
                softStart -= cig.getLength();
            else if (op != CigarOperator.HARD_CLIP)
                break;
        }
        return softStart;
    }

    /**
     * Calculates the reference coordinate for the end of the read taking into account soft clips but not hard clips.
     *
     * Note: getUnclippedEnd() adds soft and hard clips, this function only adds soft clips.
     *
     * @return the unclipped end of the read taking soft clips (but not hard clips) into account
     */
    public static int getSoftEnd(Read read) {
        boolean foundAlignedBase = false;
        int softEnd = read.getEnd();
        final List<CigarElement> cigs = read.getCigar().getCigarElements();
        for (int i = cigs.size() - 1; i >= 0; --i) {
            final CigarElement cig = cigs.get(i);
            final CigarOperator op = cig.getOperator();

            if (op == CigarOperator.SOFT_CLIP) // assumes the soft clip that we found is at the end of the aligned read
                softEnd += cig.getLength();
            else if (op != CigarOperator.HARD_CLIP) {
                foundAlignedBase = true;
                break;
            }
        }
        if( !foundAlignedBase ) { // for example 64H14S, the soft end is actually the same as the alignment end
            softEnd = read.getEnd();
        }
        return softEnd;
    }

    public static int getReadCoordinateForReferenceCoordinate(Read read, int refCoord, ClippingTail tail) {
        return getReadCoordinateForReferenceCoordinate(getSoftStart(read), read.getCigar(), refCoord, tail, false);
    }

    public static int getReadCoordinateForReferenceCoordinateUpToEndOfRead(Read read, int refCoord, ClippingTail tail) {
        final int leftmostSafeVariantPosition = Math.max(getSoftStart(read), refCoord);
        return getReadCoordinateForReferenceCoordinate(getSoftStart(read), read.getCigar(), leftmostSafeVariantPosition, tail, false);
    }

    /**
     * Pre-processes the results of {@link #getReadCoordinateForReferenceCoordinate(int, Cigar, int, boolean)} to take care of
     * two corner cases:
     *
     * 1. If clipping the right tail (end of the read) getReadCoordinateForReferenceCoordinate and fall inside
     * a deletion return the base after the deletion. If clipping the left tail (beginning of the read) it
     * doesn't matter because it already returns the previous base by default.
     *
     * 2. If clipping the left tail (beginning of the read) getReadCoordinateForReferenceCoordinate and the
     * read starts with an insertion, and you're requesting the first read based coordinate, it will skip
     * the leading insertion (because it has the same reference coordinate as the following base).
     *
     * @return the read coordinate corresponding to the requested reference coordinate for clipping.
     */
    public static int getReadCoordinateForReferenceCoordinate(final int alignmentStart, final Cigar cigar, final int refCoord, final ClippingTail tail, final boolean allowGoalNotReached) {
        Pair<Integer, Boolean> result = getReadCoordinateForReferenceCoordinate(alignmentStart, cigar, refCoord, allowGoalNotReached);
        int readCoord = result.getLeft();

        // Corner case one: clipping the right tail and falls on deletion, move to the next
        // read coordinate. It is not a problem for the left tail because the default answer
        // from getReadCoordinateForReferenceCoordinate is to give the previous read coordinate.
        if (result.getRight() && tail == ClippingTail.RIGHT_TAIL)
            readCoord++;

        // clipping the left tail and first base is insertion, go to the next read coordinate
        // with the same reference coordinate. Advance to the next cigar element, or to the
        // end of the read if there is no next element.
        final CigarElement firstElementIsInsertion = readStartsWithInsertion(cigar);
        if (readCoord == 0 && tail == ClippingTail.LEFT_TAIL && firstElementIsInsertion != null)
            readCoord = Math.min(firstElementIsInsertion.getLength(), cigar.getReadLength() - 1);

        return readCoord;
    }

    public static Pair<Integer, Boolean> getReadCoordinateForReferenceCoordinate(final int alignmentStart, final Cigar cigar, final int refCoord, final boolean allowGoalNotReached) {
        int readBases = 0;
        int refBases = 0;
        boolean fallsInsideDeletionOrSkippedRegion = false;
        boolean endJustBeforeDeletionOrSkippedRegion = false;
        boolean fallsInsideOrJustBeforeDeletionOrSkippedRegion = false;

        final int goal = refCoord - alignmentStart;  // The goal is to move this many reference bases
        if (goal < 0) {
            if (allowGoalNotReached) {
                return new MutablePair<>(CLIPPING_GOAL_NOT_REACHED, false);
            } else {
                throw new GATKException("Somehow the requested coordinate is not covered by the read. Too many deletions?");
            }
        }
        boolean goalReached = refBases == goal;

        Iterator<CigarElement> cigarElementIterator = cigar.getCigarElements().iterator();
        while (!goalReached && cigarElementIterator.hasNext()) {
            final CigarElement cigarElement = cigarElementIterator.next();
            int shift = 0;

            if (cigarElement.getOperator().consumesReferenceBases() || cigarElement.getOperator() == CigarOperator.SOFT_CLIP) {
                if (refBases + cigarElement.getLength() < goal)
                    shift = cigarElement.getLength();
                else
                    shift = goal - refBases;

                refBases += shift;
            }
            goalReached = refBases == goal;

            if (!goalReached && cigarElement.getOperator().consumesReadBases())
                readBases += cigarElement.getLength();

            if (goalReached) {
                // Is this base's reference position within this cigar element? Or did we use it all?
                final boolean endsWithinCigar = shift < cigarElement.getLength();

                // If it isn't, we need to check the next one. There should *ALWAYS* be a next one
                // since we checked if the goal coordinate is within the read length, so this is just a sanity check.
                if (!endsWithinCigar && !cigarElementIterator.hasNext()) {
                    if (allowGoalNotReached) {
                        return new MutablePair<>(CLIPPING_GOAL_NOT_REACHED, false);
                    } else {
                        throw new GATKException(String.format("Reference coordinate corresponds to a non-existent base in the read. This should never happen -- check read with alignment start: %s  and cigar: %s", alignmentStart, cigar));
                    }
                }

                CigarElement nextCigarElement = null;

                // if we end inside the current cigar element, we just have to check if it is a deletion (or skipped region)
                if (endsWithinCigar)
                    fallsInsideDeletionOrSkippedRegion = (cigarElement.getOperator() == CigarOperator.DELETION || cigarElement.getOperator() == CigarOperator.SKIPPED_REGION) ;

                    // if we end outside the current cigar element, we need to check if the next element is an insertion, deletion or skipped region.
                else {
                    nextCigarElement = cigarElementIterator.next();

                    // if it's an insertion, we need to clip the whole insertion before looking at the next element
                    if (nextCigarElement.getOperator() == CigarOperator.INSERTION) {
                        readBases += nextCigarElement.getLength();
                        if (!cigarElementIterator.hasNext()) {
                            if (allowGoalNotReached) {
                                return new MutablePair<Integer, Boolean>(CLIPPING_GOAL_NOT_REACHED, false);
                            } else {
                                throw new GATKException(String.format("Reference coordinate corresponds to a non-existent base in the read. This should never happen -- check read with alignment start: %s  and cigar: %s", alignmentStart, cigar));
                            }
                        }

                        nextCigarElement = cigarElementIterator.next();
                    }

                    // if it's a deletion (or skipped region), we will pass the information on to be handled downstream.
                    endJustBeforeDeletionOrSkippedRegion = (nextCigarElement.getOperator() == CigarOperator.DELETION || nextCigarElement.getOperator() == CigarOperator.SKIPPED_REGION);
                }

                fallsInsideOrJustBeforeDeletionOrSkippedRegion = endJustBeforeDeletionOrSkippedRegion || fallsInsideDeletionOrSkippedRegion;

                // If we reached our goal outside a deletion (or skipped region), add the shift
                if (!fallsInsideOrJustBeforeDeletionOrSkippedRegion && cigarElement.getOperator().consumesReadBases())
                    readBases += shift;

                    // If we reached our goal just before a deletion (or skipped region) we need
                    // to add the shift of the current cigar element but go back to it's last element to return the last
                    // base before the deletion (or skipped region) (see warning in function contracts)
                else if (endJustBeforeDeletionOrSkippedRegion && cigarElement.getOperator().consumesReadBases())
                    readBases += shift - 1;

                    // If we reached our goal inside a deletion (or skipped region), or just between a deletion and a skipped region,
                    // then we must backtrack to the last base before the deletion (or skipped region)
                else if (fallsInsideDeletionOrSkippedRegion ||
                        (endJustBeforeDeletionOrSkippedRegion && nextCigarElement.getOperator().equals(CigarOperator.N)) ||
                        (endJustBeforeDeletionOrSkippedRegion && nextCigarElement.getOperator().equals(CigarOperator.D)))
                    readBases--;
            }
        }

        if (!goalReached) {
            if (allowGoalNotReached) {
                return new MutablePair<>(CLIPPING_GOAL_NOT_REACHED, false);
            } else {
                throw new GATKException("Somehow the requested coordinate is not covered by the read. Alignment " + alignmentStart + " | " + cigar);
            }
        }

        return new MutablePair<>(readBases, fallsInsideOrJustBeforeDeletionOrSkippedRegion);
    }

    /**
     * Is a base inside a read?
     *
     * @param read                the read to evaluate
     * @param referenceCoordinate the reference coordinate of the base to test
     * @return true if it is inside the read, false otherwise.
     */
    public static boolean isInsideRead(final Read read, final int referenceCoordinate) {
        return referenceCoordinate >= read.getStart() && referenceCoordinate <= read.getEnd();
    }

    /**
     * @see #readStartsWithInsertion(htsjdk.samtools.Cigar, boolean) with ignoreClipOps set to true
     */
    public static CigarElement readStartsWithInsertion(final Cigar cigarForRead) {
        return readStartsWithInsertion(cigarForRead, true);
    }

    /**
     * Checks if a read starts with an insertion.
     *
     * @param cigarForRead    the CIGAR to evaluate
     * @param ignoreSoftClipOps   should we ignore S operators when evaluating whether an I operator is at the beginning?  Note that H operators are always ignored.
     * @return the element if it's a leading insertion or null otherwise
     */
    public static CigarElement readStartsWithInsertion(final Cigar cigarForRead, final boolean ignoreSoftClipOps) {
        for ( final CigarElement cigarElement : cigarForRead.getCigarElements() ) {
            if ( cigarElement.getOperator() == CigarOperator.INSERTION )
                return cigarElement;

            else if ( cigarElement.getOperator() != CigarOperator.HARD_CLIP && ( !ignoreSoftClipOps || cigarElement.getOperator() != CigarOperator.SOFT_CLIP) )
                break;
        }
        return null;
    }

    /**
     * Returns the reverse complement of the read bases
     *
     * @param bases the read bases
     * @return the reverse complement of the read bases
     */
    public static String getBasesReverseComplement(byte[] bases) {
        String reverse = "";
        for (int i = bases.length-1; i >=0; i--) {
            reverse += (char) BaseUtils.getComplement(bases[i]);
        }
        return reverse;
    }

    /**
     * Returns the reverse complement of the read bases
     *
     * @param read the read
     * @return the reverse complement of the read bases
     */
    public static String getBasesReverseComplement(Read read) {
        return getBasesReverseComplement(read.getBases());
    }

    /**
     * Calculate the maximum read length from the given list of reads.
     * @param reads list of reads
     * @return      non-negative integer
     */
    public static int getMaxReadLength( final List<? extends Read> reads ) {
        if( reads == null ) { throw new IllegalArgumentException("Attempting to check a null list of reads."); }

        int maxReadLength = 0;
        for( final Read read : reads ) {
            maxReadLength = Math.max(maxReadLength, read.getLength());
        }
        return maxReadLength;
    }

    /**
     * Creates an "empty" MutableRead with the provided read's read group and mate
     * information, but empty (not-null) fields:
     *  - Cigar String
     *  - Read Bases
     *  - Base Qualities
     *
     * Use this method if you want to create a new empty MutableRead based on
     * another MutableRead
     *
     * @param read a read to copy fields from
     * @return a read with no bases but safe for the GATK
     */
    public static MutableRead emptyRead( final MutableRead read ) {
        final MutableRead emptyRead = (MutableRead)read.copy();

        emptyRead.setCigar("");
        emptyRead.setBases(new byte[0]);
        emptyRead.setBaseQualities(new byte[0]);

        emptyRead.clearAttributes();
        String readGroup = read.getReadGroup();
        if (readGroup != null) {
            emptyRead.setAttribute(SAMTag.RG.name(), readGroup);
        }

        return emptyRead;
    }

    public static void setInsertionBaseQualities( MutableRead read, final byte[] quals) {
        read.setAttribute(BQSR_BASE_INSERTION_QUALITIES, quals == null ? null : SAMUtils.phredToFastq(quals));
    }

    public static void setDeletionBaseQualities( MutableRead read, final byte[] quals) {
        read.setAttribute(BQSR_BASE_DELETION_QUALITIES, quals == null ? null : SAMUtils.phredToFastq(quals));
    }

    /**
     * @return whether or not this read has base insertion or deletion qualities (one of the two is sufficient to return true)
     */
    public static boolean hasBaseIndelQualities(Read read) {
        return read.hasAttribute(BQSR_BASE_INSERTION_QUALITIES) || read.hasAttribute(BQSR_BASE_DELETION_QUALITIES);
    }

    /**
     * @return the base deletion quality or null if read doesn't have one
     */
    public static byte[] getExistingBaseInsertionQualities(Read read) {
        return SAMUtils.fastqToPhred(read.getAttributeAsString(BQSR_BASE_INSERTION_QUALITIES));
    }

    /**
     * @return the base deletion quality or null if read doesn't have one
     */
    public static byte[] getExistingBaseDeletionQualities(Read read) {
        return SAMUtils.fastqToPhred( read.getAttributeAsString(BQSR_BASE_DELETION_QUALITIES));
    }

    /**
     * Default utility to query the base insertion quality of a read. If the read doesn't have one, it creates an array of default qualities (currently Q45)
     * and assigns it to the read.
     *
     * @return the base insertion quality array
     */
    public static byte[] getBaseInsertionQualities(Read read) {
        byte [] quals = getExistingBaseInsertionQualities(read);
        if( quals == null ) {
            quals = new byte[read.getBaseQualities().length];
            Arrays.fill(quals, DEFAULT_INSERTION_DELETION_QUAL); // Some day in the future when base insertion and base deletion quals exist the samtools API will
            // be updated and the original quals will be pulled here, but for now we assume the original quality is a flat Q45
        }
        return quals;
    }

    /**
     * Default utility to query the base deletion quality of a read. If the read doesn't have one, it creates an array of default qualities (currently Q45)
     * and assigns it to the read.
     *
     * @return the base deletion quality array
     */
    public static byte[] getBaseDeletionQualities(Read read) {
        byte[] quals = getExistingBaseDeletionQualities(read);
        if( quals == null ) {
            quals = new byte[read.getBaseQualities().length];
            Arrays.fill(quals, DEFAULT_INSERTION_DELETION_QUAL);  // Some day in the future when base insertion and base deletion quals exist the samtools API will
            // be updated and the original quals will be pulled here, but for now we assume the original quality is a flat Q45
        }
        return quals;
    }

    public static byte[] getBaseQualities( final Read read, final EventType errorModel ) {
        switch( errorModel ) {
            case BASE_SUBSTITUTION:
                return read.getBaseQualities();
            case BASE_INSERTION:
                return getBaseInsertionQualities(read);
            case BASE_DELETION:
                return getBaseDeletionQualities(read);
            default:
                throw new GATKException("Unrecognized Base Recalibration type: " + errorModel );
        }
    }

    /**
     * Setters and Accessors for base insertion and base deletion quality scores
     */
    public static void setBaseQualities( final MutableRead read, final byte[] quals, final EventType errorModel ) {
        switch( errorModel ) {
            case BASE_SUBSTITUTION:
                read.setBaseQualities(quals);
                break;
            case BASE_INSERTION:
                read.setAttribute(BQSR_BASE_INSERTION_QUALITIES, quals == null ? null : SAMUtils.phredToFastq(quals));
                break;
            case BASE_DELETION:
                read.setAttribute(BQSR_BASE_DELETION_QUALITIES, quals == null ? null : SAMUtils.phredToFastq(quals) );
                break;
            default:
                throw new GATKException("Unrecognized Base Recalibration type: " + errorModel );
        }
    }

    /**
     * is the read a SOLiD read?
     *
     * @param read the read to test
     * @param header SAM header for the read
     * @return checks the read group tag PL for the default SOLiD tag
     */
    public static boolean isSOLiDRead(final Read read, final SAMFileHeader header) {
        return NGSPlatform.fromReadGroupPL(ReadUtils.getPlatformForRead(read, header)) == NGSPlatform.SOLID;
    }

    /**
     * Resets the quality scores of the reads to the orginal (pre-BQSR) ones.
     */
    public static MutableRead resetOriginalBaseQualities(MutableRead read) {
        byte[] originalQuals = ReadUtils.getOriginalBaseQualities(read);
        if ( originalQuals != null )
            read.setBaseQualities(originalQuals);
        return read;
    }

    /**
     * Check to ensure that the alignment makes sense based on the contents of the header.
     * @param header The SAM file header.
     * @param read The read to verify.
     * @return true if alignment agrees with header, false otherwise.
     */
    public static boolean alignmentAgreesWithHeader(final SAMFileHeader header, final Read read) {
        final int referenceIndex = getReferenceIndex(read, header);
        // Read is aligned to nonexistent contig
        if( referenceIndex == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX && read.getStart() != SAMRecord.NO_ALIGNMENT_START )
            return false;
        final SAMSequenceRecord contigHeader = header.getSequence(referenceIndex);
        // Read is aligned to a point after the end of the contig
        if( !read.isUnmapped() && read.getStart() > contigHeader.getSequenceLength() )
            return false;
        return true;
    }

}
