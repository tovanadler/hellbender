package org.broadinstitute.hellbender.utils.read;

import com.google.cloud.dataflow.sdk.coders.StandardCoder;
import htsjdk.samtools.Cigar;
import htsjdk.samtools.util.Locatable;

import java.io.Serializable;
import java.util.UUID;

public interface Read extends Locatable, Serializable {
    String getName();

    int getLength();

    byte[] getBases();

    byte[] getBaseQualities();

    Cigar getCigar();

    boolean isUnmapped();

    UUID getUUID();
}
