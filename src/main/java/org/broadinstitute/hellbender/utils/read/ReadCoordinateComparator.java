package org.broadinstitute.hellbender.utils.read;

import htsjdk.samtools.SAMFileHeader;

import java.util.Comparator;

/**
 * Comparator for sorting Reads by coordinate. Note that a header is required in
 * order to meaningfully compare contigs.
 *
 * Uses the various other fields in a Read to break ties for Reads that share
 * the same location.
 *
 * Based loosely on the SAMRecordCoordinateComparator from htsjdk.
 */
public class ReadCoordinateComparator implements Comparator<Read> {

    private SAMFileHeader header;

    public ReadCoordinateComparator( final SAMFileHeader header ) {
        this.header = header;
    }

    @Override
    public int compare( Read first, Read second ) {
        int result = compareCoordinates(first, second);
        if ( result != 0 ) {
            return result;
        }

        result = first.getName().compareTo(second.getName());
        if ( result != 0 ) { return result; }
        result = compareInts(ReadUtils.getSAMFlagsForRead(first), ReadUtils.getSAMFlagsForRead(second));
        if ( result != 0 ) { return result; }
        result = compareInts(first.getMappingQuality(), second.getMappingQuality());
        if ( result != 0 ) { return result; }
        result = compareInts(ReadUtils.getMateReferenceIndex(first, header), ReadUtils.getMateReferenceIndex(second, header));
        if ( result != 0 ) { return result; }
        result = compareInts(first.getMateStart(), second.getMateStart());
        if ( result != 0 ) { return result; }
        result = compareInts(first.getFragmentLength(), second.getFragmentLength());

        return result;
    }

    private int compareCoordinates( final Read first, final Read second ) {
        final int firstRefIndex = ReadUtils.getReferenceIndex(first, header);
        final int secondRefIndex = ReadUtils.getReferenceIndex(second, header);

        if ( first.isUnmapped() ) {
            return second.isUnmapped() ? 0 : 1;
        }
        else if ( second.isUnmapped() ) {
            return -1;
        }

        final int refIndexComparison = firstRefIndex - secondRefIndex;
        if ( refIndexComparison != 0 ) {
            return refIndexComparison;
        }

        return first.getStart() - second.getStart();
    }

    private int compareInts(int i1, int i2) {
        return i1 < i2 ? -1 : (i1 > i2 ? 1 : 0);
    }
}
