package org.broadinstitute.hellbender.dev.tools.walkers.bqsr;

import com.google.api.services.genomics.model.Read;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.readers.bam.ReadConverter;
import com.sun.security.auth.UserPrincipal;
import htsjdk.samtools.SAMException;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.util.CloserUtil;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.ArgumentCollection;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.argumentcollections.IntervalArgumentCollection;
import org.broadinstitute.hellbender.cmdline.argumentcollections.OptionalIntervalArgumentCollection;
import org.broadinstitute.hellbender.cmdline.argumentcollections.RequiredReadInputArgumentCollection;
import org.broadinstitute.hellbender.cmdline.programgroups.ReadProgramGroup;
import org.broadinstitute.hellbender.dev.pipelines.bqsr.ApplyBQSRTransform;
import org.broadinstitute.hellbender.dev.pipelines.bqsr.BaseRecalOutput;
import org.broadinstitute.hellbender.dev.pipelines.bqsr.SmallBamWriter;
import org.broadinstitute.hellbender.engine.FeatureContext;
import org.broadinstitute.hellbender.engine.ReadWalker;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.engine.dataflow.DataflowCommandLineProgram;
import org.broadinstitute.hellbender.engine.dataflow.ReadsSource;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.ApplyBQSRArgumentCollection;
import org.broadinstitute.hellbender.transformers.BQSRReadTransformer;
import org.broadinstitute.hellbender.transformers.ReadTransformer;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.dataflow.BucketUtils;
import org.broadinstitute.hellbender.utils.dataflow.DataflowUtils;
import org.broadinstitute.hellbender.utils.read.ReadUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

@CommandLineProgramProperties(
        usage = "Applies the BQSR table to the input BAM.",
        usageShort = "ApplyBQSR -bqsr RECAL_TABLE -I input -O output",
        programGroup = ReadProgramGroup.class
)
public final class ApplyBQSRDataflow extends DataflowCommandLineProgram {

    @ArgumentCollection
    public final RequiredReadInputArgumentCollection readArguments = new RequiredReadInputArgumentCollection();

    @ArgumentCollection
    protected IntervalArgumentCollection intervalArgumentCollection = new OptionalIntervalArgumentCollection();

    /**
     * Output path. Can be local or gs://
     */
    @Argument(fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME, shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME, doc="Write output to this file")
    public String OUTPUT;

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

    private SAMFileHeader header;


    protected void setupPipeline(Pipeline pipeline) {
        if (readArguments.getReadFilesNames().size()>1) {
            throw new UserException("Sorry, we only support a single input file for now.");
        }
        String filename = readArguments.getReadFilesNames().get(0);
        PCollection<Read> input;
        try {
            input = ingestReadsAndGrabHeader(pipeline, filename);
        } catch (IOException x) {
            throw new UserException.CouldNotReadInputFile(filename, x);
        }
        // Surely we'll be using this commented-out code once ReadsSource takes stringency into account.
        //ReadsSource readsSource = new ReadsSource(filename, pipeline);
        //SAMFileHeader header = readsSource.getHeader();
        //final SAMSequenceDictionary sequenceDictionary = header.getSequenceDictionary();
        //final List<SimpleInterval> intervals = intervalArgumentCollection.intervalsSpecified() ? intervalArgumentCollection.getIntervals(sequenceDictionary) :
        //        IntervalUtils.getAllIntervalsForReference(sequenceDictionary);
        final BaseRecalOutput recalInfo = new BaseRecalOutput(BQSR_RECAL_FILE);
        PCollection<BaseRecalOutput> recalInfoSingletonCollection = pipeline.apply(Create.of(recalInfo).withName("recal_file ingest"));
        PCollection<Read> output = input // readsSource.getReadPCollection(intervals, ValidationStringency.SILENT)
            .apply(new ApplyBQSRTransform(header, recalInfoSingletonCollection, bqsrOpts));
        SmallBamWriter.writeToFile(pipeline, output, header, OUTPUT);
        output.apply(DataflowUtils.convertToString()).apply(TextIO.Write.to("temp-abqsr-output.txt"));
    }

    /** reads local disks or GCS -> header, and PCollection */
    private PCollection<Read> ingestReadsAndGrabHeader(final Pipeline pipeline, String filename) throws IOException {

        // input reads
        if (BucketUtils.isCloudStorageUrl(filename)) {
            // set up ingestion on the cloud
            // but read the header locally
            GcsPath path = GcsPath.fromUri(filename);
            InputStream inputstream = Channels.newInputStream(new GcsUtil.GcsUtilFactory().create(pipeline.getOptions())
                    .open(path));
            SamReader reader = SamReaderFactory.makeDefault().validationStringency(ValidationStringency.SILENT).open(SamInputResource.of(inputstream));
            header = reader.getFileHeader();

            final SAMSequenceDictionary sequenceDictionary = header.getSequenceDictionary();
            final List<SimpleInterval> intervals = intervalArgumentCollection.intervalsSpecified() ? intervalArgumentCollection.getIntervals(sequenceDictionary) :
                    IntervalUtils.getAllIntervalsForReference(sequenceDictionary);
            return new ReadsSource(filename, pipeline).getReadPCollection(intervals, ValidationStringency.SILENT);
        } else {
            // ingestion from local file
            SamReader reader = SamReaderFactory.makeDefault().validationStringency(ValidationStringency.SILENT).open(new File(filename));
            header = reader.getFileHeader();
            List<Read> readLst = new ArrayList<>();
            for (SAMRecord sr : reader) {
                try {
                    Read e = ReadConverter.makeRead(sr);
                    readLst.add(e);
                } catch (SAMException x) {
                    logger.warn("Skipping read " + sr.getReadName() + " because we can't convert it. "+x.getMessage());
                } catch (NullPointerException y) {
                    logger.warn("Skipping read " + sr.getReadName() + " because we can't convert it. (null?)");
                }
            }
            logger.info("Input contains "+readLst.size()+" reads.");
            return pipeline.apply(Create.of(readLst).withName("input ingest"));
        }
    }

}
