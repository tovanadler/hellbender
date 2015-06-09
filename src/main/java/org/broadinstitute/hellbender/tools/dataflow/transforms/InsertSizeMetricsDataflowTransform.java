package org.broadinstitute.hellbender.tools.dataflow.transforms;

import com.google.api.services.genomics.model.Read;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.common.collect.Sets;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.metrics.Header;
import htsjdk.samtools.metrics.MetricBase;
import htsjdk.samtools.metrics.MetricsFile;
import htsjdk.samtools.util.Histogram;
import htsjdk.samtools.util.Log;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.ArgumentCollectionDefinition;
import org.broadinstitute.hellbender.engine.dataflow.DataFlowSAMFn;
import org.broadinstitute.hellbender.engine.dataflow.PTransformSAM;
import org.broadinstitute.hellbender.engine.dataflow.SAMSerializableFunction;
import org.broadinstitute.hellbender.engine.filters.ReadFilter;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.metrics.MetricAccumulationLevel;
import org.broadinstitute.hellbender.tools.picard.analysis.InsertSizeMetrics;

import java.io.Serializable;
import java.io.StringWriter;
import java.util.*;

public class InsertSizeMetricsDataflowTransform extends PTransformSAM<InsertSizeMetricsDataflowTransform.MetricsFileDataflow<InsertSizeMetrics,Integer>> {
    private final Arguments args;

    public static class Arguments implements ArgumentCollectionDefinition, Serializable {
        @Argument(doc = "Generate mean, sd and plots by trimming the data down to MEDIAN + DEVIATIONS*MEDIAN_ABSOLUTE_DEVIATION. " +
                "This is done because insert size data typically includes enough anomalous values from chimeras and other " +
                "artifacts to make the mean and sd grossly misleading regarding the real distribution.")
        public double DEVIATIONS = 10;

        @Argument(shortName = "W", doc = "Explicitly sets the Histogram width, overriding automatic truncation of Histogram tail. " +
                "Also, when calculating mean and standard deviation, only bins <= HISTOGRAM_WIDTH will be included.", optional = true)
        public Integer HISTOGRAM_WIDTH = null;

        @Argument(shortName = "M", doc = "When generating the Histogram, discard any data categories (out of FR, TANDEM, RF) that have fewer than this " +
                "percentage of overall reads. (Range: 0 to 0.5).")
        public float MINIMUM_PCT = 0.05f;

        @Argument(shortName = "LEVEL", doc = "The level(s) at which to accumulate metrics.  ")
        private Set<MetricAccumulationLevel> METRIC_ACCUMULATION_LEVEL = EnumSet.of(MetricAccumulationLevel.ALL_READS);

        @Override
        public void validate() {
            if (MINIMUM_PCT < 0 || MINIMUM_PCT > 0.5) {
                throw new UserException.BadArgumentValue("MINIMUM_PCT", "It must be between 0 and 0.5 so all data categories don't get discarded.");
            }
        }
    }

    private static final Log log = Log.getInstance(InsertSizeMetricsDataflowTransform.class);

    public InsertSizeMetricsDataflowTransform(Arguments args) {
        this.args = args;
    }

    @Override
    public PCollection<MetricsFileDataflow<InsertSizeMetrics, Integer>> apply(PCollection<Read> input) {

        input.getPipeline().getCoderRegistry().registerCoder(InsertSizeMetrics.class, SerializableCoder.of(InsertSizeMetrics.class));

        PCollection<Read> filtered = input.apply(Filter.by(new SAMSerializableFunction<>(getHeader(), isSecondInMappedPair))).setName("Filter singletons and first of pair");

        PCollection<KV<InsertSizeAggregationLevel, Integer>> kvPairs = filtered.apply(ParDo.of(new DataFlowSAMFn<KV<InsertSizeAggregationLevel, Integer>>(getHeader()) {
            @Override
            protected void apply(SAMRecord read) {
                Integer metric = computeMetric(read);
                List<InsertSizeAggregationLevel> aggregationLevels = InsertSizeAggregationLevel.getKeysForAllAggregationLevels(read, true, true, true, true);

                aggregationLevels.stream().forEach(k -> output(KV.of(k,metric)));
            }
        })).setName("Calculate metric and key")
                .setCoder(KvCoder.of(GenericJsonCoder.of(InsertSizeAggregationLevel.class),BigEndianIntegerCoder.of()));

        CombineFn<Integer, DataflowHistogram<Integer>, DataflowHistogram<Integer>> combiner = new DataflowHistogrammer<>();


        PCollection<KV<InsertSizeAggregationLevel,DataflowHistogram<Integer>>> histograms = kvPairs.apply(Combine.<InsertSizeAggregationLevel, Integer,DataflowHistogram<Integer>>perKey(combiner)).setName("Add reads to histograms");
        PCollection<KV<InsertSizeAggregationLevel, KV<InsertSizeAggregationLevel,DataflowHistogram<Integer>>>> reKeyedHistograms = histograms.apply(ParDo.of(new DoFn<KV<InsertSizeAggregationLevel,DataflowHistogram<Integer>>,KV<InsertSizeAggregationLevel,KV<InsertSizeAggregationLevel,DataflowHistogram<Integer>>>>() {

            @Override
            public void processElement(ProcessContext c) throws Exception {
                KV<InsertSizeAggregationLevel, DataflowHistogram<Integer>> histo = c.element();
                InsertSizeAggregationLevel oldKey = histo.getKey();
                InsertSizeAggregationLevel newKey = new InsertSizeAggregationLevel(null, oldKey.getLibrary(), oldKey.getReadGroup(), oldKey.getSample());
                c.output(KV.of(newKey, histo));
            }
        })).setName("Re-key histograms");

        PCollection <KV<InsertSizeAggregationLevel,MetricsFileDataflow < InsertSizeMetrics, Integer >>> metricsFiles = reKeyedHistograms.apply(Combine.perKey(new CombineHistogramsIntoMetricsFile(args.DEVIATIONS, args.HISTOGRAM_WIDTH, args.MINIMUM_PCT)))
                //.setCoder(SerializableCoder.of((Class<MetricsFileDataflow<InsertSizeMetrics, Integer>>) new MetricsFileDataflow<InsertSizeMetrics, Integer>().getClass()))
                .setName("Add histograms and metrics to MetricsFile");

        PCollection<MetricsFileDataflow < InsertSizeMetrics, Integer >> metricsFilesNoKeys = metricsFiles.apply(ParDo.of(new DoFn<KV<?, MetricsFileDataflow<InsertSizeMetrics, Integer>>, MetricsFileDataflow<InsertSizeMetrics, Integer>>() {

            @Override
            public void processElement(ProcessContext c) throws Exception {
                c.output(c.element().getValue());
            }
        })).setName("Drop keys");

        PCollection<MetricsFileDataflow<InsertSizeMetrics,Integer>> singleMetricsFile = metricsFilesNoKeys.<PCollection<MetricsFileDataflow<InsertSizeMetrics, Integer>>>apply(Combine.<MetricsFileDataflow<InsertSizeMetrics, Integer>, MetricsFileDataflow<InsertSizeMetrics, Integer>>globally(new CombineMetricsFiles()));
        return singleMetricsFile;
    }


    public static class DataflowHistogrammer<K extends Comparable<K>> extends Combine.AccumulatingCombineFn<K, DataflowHistogram<K>, DataflowHistogram<K>>{
        @Override
        public DataflowHistogram<K> createAccumulator() {
            return new DataflowHistogram<>();
        }
    }




    public static class MetricsFileDataflow<BEAN extends MetricBase & Serializable , HKEY extends Comparable> extends MetricsFile<BEAN, HKEY> implements Serializable {
        @Override
        public String toString(){
            StringWriter writer = new StringWriter();
            write(writer);
            return writer.toString();
        }
    }


    ReadFilter isSecondInMappedPair = r -> r.getReadPairedFlag() &&
            !r.getReadUnmappedFlag() &&
            !r.getMateUnmappedFlag() &&
            !r.getFirstOfPairFlag() &&
            !r.isSecondaryOrSupplementary() &&
            !r.getDuplicateReadFlag() &&
            r.getInferredInsertSize() != 0;


    private Integer computeMetric(SAMRecord read) {
        return Math.abs(read.getInferredInsertSize());
    }

    public static class  CombineMetricsFiles
    extends Combine.CombineFn<MetricsFileDataflow<InsertSizeMetrics,Integer>, MetricsFileDataflow<InsertSizeMetrics,Integer>, MetricsFileDataflow<InsertSizeMetrics,Integer>> {

        @Override
        public MetricsFileDataflow<InsertSizeMetrics,Integer> createAccumulator() {
            return new MetricsFileDataflow<>();
        }

        @Override
        public MetricsFileDataflow<InsertSizeMetrics,Integer> addInput(MetricsFileDataflow<InsertSizeMetrics,Integer> accumulator, MetricsFileDataflow<InsertSizeMetrics,Integer> input) {
            return combineMetricsFiles(accumulator, input);
        }

        private MetricsFileDataflow<InsertSizeMetrics,Integer> combineMetricsFiles(MetricsFileDataflow<InsertSizeMetrics,Integer> accumulator, MetricsFileDataflow<InsertSizeMetrics,Integer> input) {
            Set<Header> headers = Sets.newLinkedHashSet(accumulator.getHeaders());
            Set<Header> inputHeaders = Sets.newLinkedHashSet(input.getHeaders());
            inputHeaders.removeAll(headers);
            inputHeaders.stream().forEach(accumulator::addHeader);

            accumulator.addAllMetrics(input.getMetrics());
            input.getAllHistograms().stream().forEach(accumulator::addHistogram);
            return accumulator;
        }

        @Override
        public MetricsFileDataflow<InsertSizeMetrics,Integer> mergeAccumulators(Iterable<MetricsFileDataflow<InsertSizeMetrics,Integer>> accumulators) {
            MetricsFileDataflow<InsertSizeMetrics,Integer> base = createAccumulator();
            accumulators.forEach(accum -> combineMetricsFiles(base,  accum));
            return base;
        }

        @Override
        public MetricsFileDataflow<InsertSizeMetrics,Integer> extractOutput(MetricsFileDataflow<InsertSizeMetrics,Integer> accumulator) {
            List<InsertSizeMetrics> metrics = new ArrayList<>(accumulator.getMetrics());
            metrics.sort(InsertSizeSorting);
            MetricsFileDataflow<InsertSizeMetrics, Integer> sorted = new MetricsFileDataflow<>();
            sorted.addAllMetrics(metrics);
            accumulator.getAllHistograms().stream().sorted(Comparator.comparing(Histogram::getValueLabel)).forEach(sorted::addHistogram);
            accumulator.getHeaders().stream().forEach(sorted::addHeader);
            return sorted;
        }

        private static final Comparator<InsertSizeMetrics> InsertSizeSorting = Comparator.comparing((InsertSizeMetrics a) -> a.SAMPLE != null ? a.SAMPLE : "")
                .thenComparing(a -> a.READ_GROUP != null ? a.READ_GROUP : "")
                .thenComparing(a -> a.LIBRARY != null ? a.LIBRARY : "");




    }


}
