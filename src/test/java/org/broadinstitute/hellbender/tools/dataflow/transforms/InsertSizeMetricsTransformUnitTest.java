package org.broadinstitute.hellbender.tools.dataflow.transforms;

import com.google.api.services.genomics.model.Read;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.common.collect.Lists;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.metrics.Header;
import htsjdk.samtools.metrics.StringHeader;
import org.broadinstitute.hellbender.engine.dataflow.ReadsSource;
import org.broadinstitute.hellbender.tools.picard.analysis.InsertSizeMetrics;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.ArtificialSAMUtils;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;



public final class InsertSizeMetricsTransformUnitTest{

    @Test(groups = "dataflow")
    public void testInsertSizeMetricsTransform(){
        File bam = new File(BaseTest.publicTestDir, "org/broadinstitute/hellbender/tools/picard/analysis/CollectInsertSizeMetrics/insert_size_metrics_test.bam");
        Pipeline p = TestPipeline.create();
        p.getCoderRegistry().registerCoder(InsertSizeMetricsDataflowTransform.MetricsFileDataflow.class, SerializableCoder.of(InsertSizeMetricsDataflowTransform.MetricsFileDataflow.class));
        DataflowWorkarounds.registerCoder(p,DataflowHistogram.class, SerializableCoder.of(DataflowHistogram.class) );
        DataflowWorkarounds.registerGenomicsCoders(p);
        List<SimpleInterval> intervals = Lists.newArrayList(new SimpleInterval("1", 1, 249250621));

        ReadsSource source = new ReadsSource(bam.getAbsolutePath(), p);
        PCollection<Read> preads = source.getReadPCollection(intervals);


        InsertSizeMetricsDataflowTransform transform = new InsertSizeMetricsDataflowTransform(new InsertSizeMetricsDataflowTransform.Arguments());
        transform.setHeader(source.getHeader());

        PCollection<InsertSizeMetricsDataflowTransform.MetricsFileDataflow<InsertSizeMetrics,Integer>> presult = preads.apply(transform);
        //presult.apply(ParDo.of(new MetricsFileDataflowBooleanDoFn()));
        DirectPipelineRunner.EvaluationResults result = (DirectPipelineRunner.EvaluationResults)p.run();
        Assert.assertEquals(result.getPCollection(presult).get(0).toString(), "## htsjdk.samtools.metrics.StringHeader\n" +
                "# org.broadinstitute.hellbender.tools.picard.analysis.CollectInsertSizeMetrics  --HISTOGRAM_FILE output.pdf --METRIC_ACCUMULATION_LEVEL ALL_READS --METRIC_ACCUMULATION_LEVEL READ_GROUP --METRIC_ACCUMULATION_LEVEL LIBRARY --METRIC_ACCUMULATION_LEVEL SAMPLE --PRODUCE_PLOT true --INPUT src/test/resources/org/broadinstitute/hellbender/tools/picard/analysis/CollectInsertSizeMetrics/insert_size_metrics_test.bam --OUTPUT metrics.out  --DEVIATIONS 10.0 --MINIMUM_PCT 0.05 --ASSUME_SORTED true --STOP_AFTER 0 --VALIDATION_STRINGENCY STRICT --COMPRESSION_LEVEL 5 --MAX_RECORDS_IN_RAM 500000 --CREATE_INDEX false --CREATE_MD5_FILE false --help false --version false --VERBOSITY INFO --QUIET false\n" +
                "## htsjdk.samtools.metrics.StringHeader\n" +
                "# Started on: Tue May 19 14:52:18 EDT 2015\n" +
                "\n" +
                "## METRICS CLASS\torg.broadinstitute.hellbender.tools.picard.analysis.InsertSizeMetrics\n" +
                "MEDIAN_INSERT_SIZE\tMEDIAN_ABSOLUTE_DEVIATION\tMIN_INSERT_SIZE\tMAX_INSERT_SIZE\tMEAN_INSERT_SIZE\tSTANDARD_DEVIATION\tREAD_PAIRS\tPAIR_ORIENTATION\tWIDTH_OF_10_PERCENT\tWIDTH_OF_20_PERCENT\tWIDTH_OF_30_PERCENT\tWIDTH_OF_40_PERCENT\tWIDTH_OF_50_PERCENT\tWIDTH_OF_60_PERCENT\tWIDTH_OF_70_PERCENT\tWIDTH_OF_80_PERCENT\tWIDTH_OF_90_PERCENT\tWIDTH_OF_99_PERCENT\tSAMPLE\tLIBRARY\tREAD_GROUP\n" +
                "41\t3\t36\t45\t40.076923\t3.121472\t13\tFR\t1\t1\t1\t7\t7\t7\t9\t11\t11\t11\n" +
                "41\t3\t36\t45\t40.076923\t3.121472\t13\tFR\t1\t1\t1\t7\t7\t7\t9\t11\t11\t11\tNA12878\n" +
                "38.5\t2.5\t36\t41\t38.5\t3.535534\t2\tFR\t5\t5\t5\t5\t5\t0\t0\t0\t0\t0\tNA12878\tSolexa-41734\n" +
                "40\t2\t36\t45\t39.555556\t2.877113\t9\tFR\t1\t3\t3\t3\t5\t5\t9\t9\t11\t11\tNA12878\tSolexa-41748\n" +
                "44\t0\t44\t44\t44\t0\t2\tFR\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\tNA12878\tSolexa-41753\n" +
                "36\t0\t36\t36\t36\t?\t1\tFR\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\tNA12878\tSolexa-41734\t62A79AAXX100907.3\n" +
                "41\t0\t41\t41\t41\t?\t1\tFR\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\tNA12878\tSolexa-41734\t62A79AAXX100907.5\n" +
                "41\t1\t38\t45\t41\t2.54951\t5\tFR\t1\t1\t1\t1\t3\t3\t7\t7\t9\t9\tNA12878\tSolexa-41748\t62A79AAXX100907.6\n" +
                "37\t1\t36\t41\t37.75\t2.362908\t4\tFR\t3\t3\t3\t3\t3\t3\t3\t9\t9\t9\tNA12878\tSolexa-41748\t62A79AAXX100907.7\n" +
                "44\t0\t44\t44\t44\t0\t2\tFR\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\tNA12878\tSolexa-41753\t62A79AAXX100907.8\n" +
                "\n" +
                "## HISTOGRAM\tjava.lang.Integer\n" +
                "insert_size\tAll_Reads.fr_count\tNA12878.fr_count\tSolexa-41734.fr_count\tSolexa-41748.fr_count\tSolexa-41753.fr_count\t62A79AAXX100907.3.fr_count\t62A79AAXX100907.5.fr_count\t62A79AAXX100907.6.fr_count\t62A79AAXX100907.7.fr_count\t62A79AAXX100907.8.fr_count\n" +
                "36\t3\t3\t1\t2\t0\t1\t0\t0\t2\t0\n" +
                "38\t2\t2\t0\t2\t0\t0\t0\t1\t1\t0\n" +
                "40\t1\t1\t0\t1\t0\t0\t0\t1\t0\t0\n" +
                "41\t4\t4\t1\t3\t0\t0\t1\t2\t1\t0\n" +
                "44\t2\t2\t0\t0\t2\t0\t0\t0\t0\t2\n" +
                "45\t1\t1\t0\t1\t0\t0\t0\t1\t0\t0");

    }


    @Test
    public void testHistogrammer(){
        List<Integer> records = IntStream.rangeClosed(1,1000).boxed().collect(Collectors.toList());
        Combine.CombineFn<Integer, DataflowHistogram<Integer>, DataflowHistogram<Integer>> combiner = new InsertSizeMetricsDataflowTransform.DataflowHistogrammer<>();

        DataflowHistogram<Integer> result = combiner.apply(records);
        Assert.assertEquals(result.getCount(),1000.0);
        Assert.assertEquals(result.getMax(),1000.0);
        Assert.assertEquals(result.getMin(),1.0);
    }

    @Test(groups = "dataflow")
    public void testHistogramCombiner(){
        Combine.CombineFn<KV<InsertSizeMetricsDataflowTransform.AggregationLevel, DataflowHistogram<Integer>>,?, InsertSizeMetricsDataflowTransform.MetricsFileDataflow<InsertSizeMetrics,Integer>> combiner = new InsertSizeMetricsDataflowTransform.CombineHistogramsIntoMetricsFile(10.0, null, 0.05f);
        SAMRecord read1 = ArtificialSAMUtils.createPair(ArtificialSAMUtils.createArtificialSamHeader(), "Read1", 100, 4, 200, true, false).get(0);
        InsertSizeMetricsDataflowTransform.AggregationLevel aggregationLevel = new InsertSizeMetricsDataflowTransform.AggregationLevel(read1, false, false, false);

        DataflowHistogram<Integer> h1= new DataflowHistogram<>();
        h1.addInput(10);
        h1.addInput(20);
        h1.addInput(10);

        DataflowHistogram<Integer> h2= new DataflowHistogram<>();
        h2.addInput(10);
        h2.addInput(20);
        h2.addInput(10);

        DataflowHistogram<Integer> h3= new DataflowHistogram<>();
        h3.addInput(10);
        h3.addInput(20);
        h3.addInput(10);

        List<DataflowHistogram<Integer>> histograms = Lists.newArrayList(h1, h2, h3);

        List<KV<InsertSizeMetricsDataflowTransform.AggregationLevel,DataflowHistogram<Integer>>> keyedHistograms = histograms.stream().map(h -> KV.of(aggregationLevel, h)).collect(Collectors.toList());
        InsertSizeMetricsDataflowTransform.MetricsFileDataflow<InsertSizeMetrics, Integer> result = combiner.apply(keyedHistograms);
        Assert.assertEquals(result.getAllHistograms().size(), 3);
    }

    @Test
    public void testCombineMetricsFiles(){
        Combine.CombineFn<InsertSizeMetricsDataflowTransform.MetricsFileDataflow<InsertSizeMetrics,Integer>,
                InsertSizeMetricsDataflowTransform.MetricsFileDataflow<InsertSizeMetrics,Integer>,
                InsertSizeMetricsDataflowTransform.MetricsFileDataflow<InsertSizeMetrics,Integer>> combiner =
                new InsertSizeMetricsDataflowTransform.CombineMetricsFiles<>();

        InsertSizeMetricsDataflowTransform.MetricsFileDataflow<InsertSizeMetrics,Integer> mf1 = new InsertSizeMetricsDataflowTransform.MetricsFileDataflow<>();
        mf1.addMetric(new InsertSizeMetrics());
        mf1.addHeader(new StringHeader("header1"));

        InsertSizeMetricsDataflowTransform.MetricsFileDataflow<InsertSizeMetrics,Integer> mf2 = new InsertSizeMetricsDataflowTransform.MetricsFileDataflow<>();
        mf2.addMetric(new InsertSizeMetrics());
        mf2.addHeader(new StringHeader("header2"));

        InsertSizeMetricsDataflowTransform.MetricsFileDataflow<InsertSizeMetrics,Integer> mf3 = new InsertSizeMetricsDataflowTransform.MetricsFileDataflow<>();
        mf3.addMetric(new InsertSizeMetrics());
        mf3.addHeader(new StringHeader("header3"));

        InsertSizeMetricsDataflowTransform.MetricsFileDataflow<InsertSizeMetrics, Integer> combined = combiner.apply(Lists.newArrayList(mf1, mf2, mf3));

        Assert.assertEquals(combined.getMetrics().size(), 3);
        Assert.assertEquals(combined.getAllHistograms().size(), 0);
        Assert.assertEquals(combined.getHeaders().size(), 3);

    }

    private static class MetricsFileDataflowBooleanDoFn extends DoFn<InsertSizeMetricsDataflowTransform.MetricsFileDataflow<InsertSizeMetrics, Integer>, Boolean> {

        @Override
        public void processElement(ProcessContext c) throws Exception {
            Assert.assertEquals(c.element().getAllHistograms().get(0).getCount(), "some string");
        }
    }

    public static class PrintLn<T> extends DoFn<T,String>{

        @Override
        public void processElement(ProcessContext c) throws Exception {
            String str = c.element().toString();
            System.out.println(str);
            c.output(str);
        }
    }

    @Test
    public void serializeMetricsFileTest(){
        InsertSizeMetricsDataflowTransform.MetricsFileDataflow<InsertSizeMetrics,Integer> metrics = new InsertSizeMetricsDataflowTransform.MetricsFileDataflow<>();
        metrics.addHistogram(new DataflowHistogram<>());

        InsertSizeMetricsDataflowTransform.MetricsFileDataflow<InsertSizeMetrics, Integer> newMetrics = SerializableUtils.ensureSerializableByCoder(SerializableCoder.of(InsertSizeMetricsDataflowTransform.MetricsFileDataflow.class), metrics, "error");
        Assert.assertEquals(newMetrics.getAllHistograms(),metrics.getAllHistograms());
    }

    @Test
    public void javaSerializeMetricsFileTest() throws IOException, ClassNotFoundException {
        final InsertSizeMetricsDataflowTransform.MetricsFileDataflow<InsertSizeMetrics,Integer> metrics = new InsertSizeMetricsDataflowTransform.MetricsFileDataflow<>();
        metrics.addHistogram(new DataflowHistogram<>());
        final ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
        final ObjectOutputStream out = new ObjectOutputStream(byteArrayStream);
        out.writeObject(metrics);
        out.close();
        final ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(byteArrayStream.toByteArray()));
        final InsertSizeMetricsDataflowTransform.MetricsFileDataflow<InsertSizeMetrics,Integer> deserializedMetrics = (InsertSizeMetricsDataflowTransform.MetricsFileDataflow<InsertSizeMetrics,Integer>)in.readObject();
        in.close();
        Assert.assertEquals(deserializedMetrics.getAllHistograms(),metrics.getAllHistograms());
    }

}
