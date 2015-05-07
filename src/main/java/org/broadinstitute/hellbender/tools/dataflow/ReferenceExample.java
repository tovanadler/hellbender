package org.broadinstitute.hellbender.tools.dataflow;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.utils.GCSOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.engine.dataflow.WorkerCredentialFactory;
import org.broadinstitute.hellbender.engine.dataflow.reference;
import org.broadinstitute.hellbender.cmdline.ArgumentCollection;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.argumentcollections.IntervalArgumentCollection;
import org.broadinstitute.hellbender.cmdline.argumentcollections.RequiredIntervalArgumentCollection;
import org.broadinstitute.hellbender.cmdline.programgroups.DataFlowProgramGroup;
import org.broadinstitute.hellbender.engine.dataflow.DataflowCommandLineProgram;
import org.broadinstitute.hellbender.engine.dataflow.reference.DoFnWithReference;
import org.broadinstitute.hellbender.engine.dataflow.reference.FastaReader;
import org.broadinstitute.hellbender.utils.SimpleInterval;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by davidada on 5/7/15.
 */

@CommandLineProgramProperties(
        usage="Count the usages of every word in a text",
        usageShort="Something",
        programGroup = DataFlowProgramGroup.class)
public class ReferenceExample extends DataflowCommandLineProgram {
    @ArgumentCollection
    protected IntervalArgumentCollection intervalArgumentCollection = new RequiredIntervalArgumentCollection();

    @Argument(fullName = "reference", shortName = "reference")
    protected String ref;

    @Argument(fullName = "output", shortName = "output")
    protected String output;

    @Argument(doc = "path to the client secret file for google cloud authentication, necessary if accessing data from buckets",
            shortName = "secret", fullName = "client_secret", optional=true)
    protected File clientSecret = new File("client_secret.json");

    @Override
    public void setupPipeline(Pipeline p) {
        GCSOptions options = PipelineOptionsFactory.fromArgs(new String[]{"--genomicsSecretsFile=" + clientSecret.getAbsolutePath()}).as(GCSOptions.class);
        GenomicsOptions.Methods.validateOptions(options);

        WorkerCredentialFactory cred = WorkerCredentialFactory.of(p);
        final List<SimpleInterval> parsedIntervals = new ArrayList<>();
        for ( String intervalString : intervalArgumentCollection.getIntervalStrings() ) {
            parsedIntervals.add(new SimpleInterval(intervalString));
        }
        p.apply(ParDo.of(new DoFnWithReference<SimpleInterval, String>(cred, fasta, fai) {
                    @Override
                    public void processElementImpl(ProcessContext context, Map<String, FastaReader.Contig> contigs) throws Exception {
                        context.output(
                                contigs.get(context.element().getContig()).get(
                                        context.element().getStart(),
                                        context.element().getEnd()));
                    }
                }))
                                // Apply a text file write transform to the PCollection of formatted word counts
                        .apply(TextIO.Write.to(output));
    }

}
