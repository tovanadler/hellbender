package org.broadinstitute.hellbender.utils.iterators;

import htsjdk.samtools.SAMRecord;
import org.broadinstitute.hellbender.utils.read.MutableRead;
import org.broadinstitute.hellbender.utils.read.SAMRecordToReadAdapter;

import java.util.Iterator;

/**
 * Wraps a SAMRecord iterator within an iterator of MutableReads.
 */
public final class SAMRecordToReadIterator implements Iterator<MutableRead>, Iterable<MutableRead> {
    private Iterator<SAMRecord> samIterator;

    public SAMRecordToReadIterator( final Iterator<SAMRecord> samIterator ) {
        this.samIterator = samIterator;
    }

    @Override
    public boolean hasNext() {
        return samIterator.hasNext();
    }

    @Override
    public MutableRead next() {
        return new SAMRecordToReadAdapter(samIterator.next());
    }

    @Override
    public Iterator<MutableRead> iterator() {
        return this;
    }
}