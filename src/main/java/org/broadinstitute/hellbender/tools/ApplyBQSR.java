package org.broadinstitute.hellbender.tools;

import com.google.api.services.genomics.model.Read;
import com.google.cloud.genomics.dataflow.readers.bam.ReadConverter;
import com.google.cloud.genomics.gatk.common.GenomicsConverter;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.util.CloserUtil;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.ArgumentCollection;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.ReadProgramGroup;
import org.broadinstitute.hellbender.engine.FeatureContext;
import org.broadinstitute.hellbender.engine.ReadWalker;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.transformers.BQSRReadTransformer;
import org.broadinstitute.hellbender.transformers.ReadTransformer;
import org.broadinstitute.hellbender.utils.read.ReadUtils;

import java.io.File;
import java.io.FileWriter;

@CommandLineProgramProperties(
        usage = "Applies the BQSR table to the input BAM.",
        usageShort = "ApplyBQSR -bqsr RECAL_TABLE -I input -O output",
        programGroup = ReadProgramGroup.class
)
public final class ApplyBQSR extends ReadWalker{

    @Argument(fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME, shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME, doc="Write output to this file")
    public File OUTPUT;

    /**
     * Enables recalibration of base qualities.
     * The covariates tables are produced by the BaseRecalibrator tool.
     * Please be aware that you should only run recalibration with the covariates file created on the same input bam(s).
     */
    @Argument(fullName="bqsr_recal_file", shortName="bqsr", doc="Input covariates table file for base quality score recalibration")
    public File BQSR_RECAL_FILE;

    /**
     * command-line options to fine tune the recalibration.
     */
    @ArgumentCollection
    public ApplyBQSRArgumentCollection bqsrOpts = new ApplyBQSRArgumentCollection();


    private SAMFileWriter outputWriter;

    private ReadTransformer transform;

    // DEBUG HACK
    private File readsDebugOutput = new File("temp-abqsr-nondataflow-output.txt");
    private FileWriter readsDebug;

    @Override
    public void onTraversalStart() {
        final SAMFileHeader outputHeader = ReadUtils.clone(getHeaderForReads());
        outputWriter = new SAMFileWriterFactory().makeWriter(outputHeader, true, OUTPUT, referenceArguments.getReferenceFile());
        transform = new BQSRReadTransformer(BQSR_RECAL_FILE, bqsrOpts.quantizationLevels, bqsrOpts.disableIndelQuals, bqsrOpts.PRESERVE_QSCORES_LESS_THAN, bqsrOpts.emitOriginalQuals, bqsrOpts.globalQScorePrior);
        try {
            readsDebug = new FileWriter(readsDebugOutput);
        } catch (Exception x) {
            throw new RuntimeException(x);
        }
    }

    @Override
    public void apply( SAMRecord read, ReferenceContext referenceContext, FeatureContext featureContext ) {
        SAMRecord transformed = transform.apply(read);
        outputWriter.addAlignment(transformed);
        Read e = ReadConverter.makeRead(transformed);
        try {
        readsDebug.write(e.toString()+"\n");
        } catch (Exception x) {
            throw new RuntimeException(x);
        }
    }

    @Override
    public Object onTraversalDone() {
        CloserUtil.close(outputWriter);
        try {
        readsDebug.close();
        } catch (Exception x) {
            throw new RuntimeException(x);
        }
        return null;
    }

}
