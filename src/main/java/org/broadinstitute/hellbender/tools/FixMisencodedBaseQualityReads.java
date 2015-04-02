package org.broadinstitute.hellbender.tools;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileWriterFactory;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.ReadProgramGroup;
import org.broadinstitute.hellbender.engine.FeatureContext;
import org.broadinstitute.hellbender.engine.ReadWalker;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.transformers.MisencodedBaseQualityReadTransformer;
import org.broadinstitute.hellbender.transformers.ReadTransformer;
import org.broadinstitute.hellbender.utils.read.MutableRead;
import org.broadinstitute.hellbender.utils.read.SAMFileReadWriter;

import java.io.File;

@CommandLineProgramProperties(
	usage = "Fixes Illumina base quality scores.",
    usageShort = "Fix Illumina base quality scores.",
    programGroup = ReadProgramGroup.class
)
public class FixMisencodedBaseQualityReads extends ReadWalker {

    @Argument(fullName = "output", shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME, doc="Write output to this file")
    public File OUTPUT;

    private SAMFileReadWriter outputWriter;

    private ReadTransformer transform;

    @Override
    public void onTraversalStart() {
        final SAMFileHeader outputHeader = getHeaderForReads().clone();
        outputWriter = new SAMFileReadWriter(new SAMFileWriterFactory().makeWriter(outputHeader, true, OUTPUT, referenceArguments.referenceFile));
        transform = new MisencodedBaseQualityReadTransformer();
    }

    @Override
    public void apply( MutableRead read, ReferenceContext referenceContext, FeatureContext featureContext ) {
        outputWriter.addRead(transform.apply(read));
    }

    @Override
    public Object onTraversalDone() {
        if ( outputWriter != null ) {
            outputWriter.close();
        }
        return null;
    }
}
