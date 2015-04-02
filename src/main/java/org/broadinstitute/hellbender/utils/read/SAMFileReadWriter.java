package org.broadinstitute.hellbender.utils.read;

import htsjdk.samtools.SAMFileWriter;
import org.broadinstitute.hellbender.exceptions.GATKException;

public class SAMFileReadWriter implements ReadWriter {

    private SAMFileWriter samWriter;

    public SAMFileReadWriter( final SAMFileWriter samWriter ) {
        this.samWriter = samWriter;
    }

    @Override
    public void addRead( Read read ) {
        if ( read.getClass() != SAMRecordToReadAdapter.class ) {
            throw new GATKException(getClass().getSimpleName() + " currently only supports writing Read objects backed by SAMRecords");
        }
        samWriter.addAlignment(((SAMRecordToReadAdapter)read).unpackSAMRecord());
    }

    @Override
    public void close() {
        samWriter.close();
    }
}
