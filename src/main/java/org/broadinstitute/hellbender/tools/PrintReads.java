package org.broadinstitute.hellbender.tools;

import htsjdk.samtools.*;
import org.broadinstitute.hellbender.cmdline.*;
import org.broadinstitute.hellbender.cmdline.programgroups.ReadProgramGroup;
import org.broadinstitute.hellbender.engine.FeatureContext;
import org.broadinstitute.hellbender.engine.ReadWalker;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.utils.read.MutableRead;
import org.broadinstitute.hellbender.utils.read.ReadUtils;
import org.broadinstitute.hellbender.utils.read.SAMFileReadWriter;

import java.io.File;

@CommandLineProgramProperties(
	usage = "Prints reads from the input to the output.",
    usageShort = "Print reads",
    programGroup = ReadProgramGroup.class
)
public class PrintReads extends ReadWalker {

    @Argument(fullName = "output", shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME, doc="Write output to this file")
    public File OUTPUT;

    private SAMFileReadWriter outputWriter;

    @Override
    public void onTraversalStart() {
        final SAMFileHeader outputHeader = ReadUtils.cloneSAMFileHeader(getHeaderForReads());
        outputWriter = new SAMFileReadWriter(new SAMFileWriterFactory().makeWriter(outputHeader, true, OUTPUT, referenceArguments.referenceFile));
    }

    @Override
    public void apply( MutableRead read, ReferenceContext referenceContext, FeatureContext featureContext ) {
        outputWriter.addRead(read);
    }

    @Override
    public Object onTraversalDone() {
        if ( outputWriter != null ) {
            outputWriter.close();
        }
        return null;
    }
}
