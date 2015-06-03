package org.broadinstitute.hellbender.tools.dataflow.transforms;

import com.google.api.client.json.GenericJson;
import com.google.api.client.util.Key;
import com.google.api.services.genomics.model.Read;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.common.collect.Sets;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamPairUtil;
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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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

        //input.getPipeline().getCoderRegistry().registerCoder(MetricsFileDataflow.class, SerializableCoder.of(MetricsFileDataflow.class));

        PCollection<Read> filtered = input.apply(Filter.by(new SAMSerializableFunction<>(getHeader(), isSecondInMappedPair))).setName("Filter singletons and first of pair");

        PCollection<KV<AggregationLevel, Integer>> kvPairs = filtered.apply(ParDo.of(new DataFlowSAMFn<KV<AggregationLevel, Integer>>(getHeader()) {
            @Override
            protected void apply(SAMRecord read) {
                Integer metric = computeMetric(read);
                List<AggregationLevel> aggregationLevels = AggregationLevel.getKeysForAllAggregationLevels(read, true, true, true, true);

                aggregationLevels.stream().forEach(k -> output(KV.of(k,metric)));
            }
        })).setName("Calculate metric and key")
                .setCoder(KvCoder.of(GenericJsonCoder.of(AggregationLevel.class),BigEndianIntegerCoder.of()));

        Combine.CombineFn<Integer, DataflowHistogram<Integer>, DataflowHistogram<Integer>> combiner = new DataflowHistogrammer<>();


        PCollection<KV<AggregationLevel,DataflowHistogram<Integer>>> histograms = kvPairs.apply(Combine.<AggregationLevel, Integer,DataflowHistogram<Integer>>perKey(combiner)).setName("Add reads to histograms");
        PCollection<KV<AggregationLevel, KV<AggregationLevel,DataflowHistogram<Integer>>>> reKeyedHistograms = histograms.apply(ParDo.of(new DoFn<KV<AggregationLevel,DataflowHistogram<Integer>>,KV<AggregationLevel,KV<AggregationLevel,DataflowHistogram<Integer>>>>() {

            @Override
            public void processElement(ProcessContext c) throws Exception {
                KV<AggregationLevel, DataflowHistogram<Integer>> histo = c.element();
                AggregationLevel oldKey = histo.getKey();
                AggregationLevel newKey = new AggregationLevel(null, oldKey.getLibrary(), oldKey.getReadGroup(), oldKey.getSample());
                c.output(KV.of(newKey, histo));
            }
        })).setName("Re-key histograms");

        PCollection <KV<AggregationLevel,MetricsFileDataflow < InsertSizeMetrics, Integer >>> metricsFiles = reKeyedHistograms.apply(Combine.perKey(new CombineHistogramsIntoMetricsFile(args.DEVIATIONS, args.HISTOGRAM_WIDTH, args.MINIMUM_PCT)))
                //.setCoder(SerializableCoder.of((Class<MetricsFileDataflow<InsertSizeMetrics, Integer>>) new MetricsFileDataflow<InsertSizeMetrics, Integer>().getClass()))
                .setName("Add histograms and metrics to MetricsFile");

        PCollection<MetricsFileDataflow < InsertSizeMetrics, Integer >> metricsFilesNoKeys = metricsFiles.apply(ParDo.of( new DoFn<KV<?,MetricsFileDataflow<InsertSizeMetrics,Integer>>,MetricsFileDataflow<InsertSizeMetrics,Integer>>(){

            @Override
            public void processElement(ProcessContext c) throws Exception {
                c.output(c.element().getValue());
            }
        })).setName("Drop keys");

        PCollection<MetricsFileDataflow<InsertSizeMetrics,Integer>> singleMetricsFile = metricsFilesNoKeys.<PCollection<MetricsFileDataflow<InsertSizeMetrics, Integer>>>apply(Combine.<MetricsFileDataflow<InsertSizeMetrics, Integer>,MetricsFileDataflow <InsertSizeMetrics, Integer>>globally(new CombineMetricsFiles<>()));
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

    public static class CombineHistogramsIntoMetricsFile
            extends Combine.CombineFn<KV<AggregationLevel,DataflowHistogram<Integer>>,CombineHistogramsIntoMetricsFile,MetricsFileDataflow<InsertSizeMetrics,Integer>> {


        private final double DEVIATIONS;
        private final Integer HISTOGRAM_WIDTH;
        private final float MINIMUM_PERCENT;

        private final Map<AggregationLevel, DataflowHistogram<Integer>> histograms = new HashMap<>();

        public CombineHistogramsIntoMetricsFile(double deviations, Integer histogramWidth, float minimumPercent) {
            this.DEVIATIONS = deviations;
            this.HISTOGRAM_WIDTH = histogramWidth;
            this.MINIMUM_PERCENT = minimumPercent;
        }

        public void  addHistogramToMetricsFile(AggregationLevel aggregationLevel, DataflowHistogram<Integer> histogram, MetricsFileDataflow<InsertSizeMetrics,Integer> metricsFile, double totalInserts) {
            final SamPairUtil.PairOrientation pairOrientation = aggregationLevel.getOrientation();
            final double total = histogram.getCount();

            // Only include a category if it has a sufficient percentage of the data in it
            if( (total > totalInserts * MINIMUM_PERCENT)) {
                final InsertSizeMetrics metrics = new InsertSizeMetrics();
                metrics.SAMPLE =  aggregationLevel.getSample();
                metrics.LIBRARY = aggregationLevel.getLibrary();
                metrics.READ_GROUP = aggregationLevel.getReadGroup();
                metrics.PAIR_ORIENTATION = pairOrientation;
                metrics.READ_PAIRS = (long) total;
                metrics.MAX_INSERT_SIZE = (int) histogram.getMax();
                metrics.MIN_INSERT_SIZE = (int) histogram.getMin();
                metrics.MEDIAN_INSERT_SIZE = histogram.getMedian();
                metrics.MEDIAN_ABSOLUTE_DEVIATION = histogram.getMedianAbsoluteDeviation();

                final double median = histogram.getMedian();
                double covered = 0;
                double low = median;
                double high = median;

                while (low >= histogram.getMin() || high <= histogram.getMax()) {
                    final htsjdk.samtools.util.Histogram<Integer>.Bin lowBin = histogram.get((int) low);
                    if (lowBin != null) covered += lowBin.getValue();

                    if (low != high) {
                        final htsjdk.samtools.util.Histogram<Integer>.Bin highBin = histogram.get((int) high);
                        if (highBin != null) covered += highBin.getValue();
                    }

                    final double percentCovered = covered / total;
                    final int distance = (int) (high - low) + 1;
                    if (percentCovered >= 0.1 && metrics.WIDTH_OF_10_PERCENT == 0)
                        metrics.WIDTH_OF_10_PERCENT = distance;
                    if (percentCovered >= 0.2 && metrics.WIDTH_OF_20_PERCENT == 0)
                        metrics.WIDTH_OF_20_PERCENT = distance;
                    if (percentCovered >= 0.3 && metrics.WIDTH_OF_30_PERCENT == 0)
                        metrics.WIDTH_OF_30_PERCENT = distance;
                    if (percentCovered >= 0.4 && metrics.WIDTH_OF_40_PERCENT == 0)
                        metrics.WIDTH_OF_40_PERCENT = distance;
                    if (percentCovered >= 0.5 && metrics.WIDTH_OF_50_PERCENT == 0)
                        metrics.WIDTH_OF_50_PERCENT = distance;
                    if (percentCovered >= 0.6 && metrics.WIDTH_OF_60_PERCENT == 0)
                        metrics.WIDTH_OF_60_PERCENT = distance;
                    if (percentCovered >= 0.7 && metrics.WIDTH_OF_70_PERCENT == 0)
                        metrics.WIDTH_OF_70_PERCENT = distance;
                    if (percentCovered >= 0.8 && metrics.WIDTH_OF_80_PERCENT == 0)
                        metrics.WIDTH_OF_80_PERCENT = distance;
                    if (percentCovered >= 0.9 && metrics.WIDTH_OF_90_PERCENT == 0)
                        metrics.WIDTH_OF_90_PERCENT = distance;
                    if (percentCovered >= 0.99 && metrics.WIDTH_OF_99_PERCENT == 0)
                        metrics.WIDTH_OF_99_PERCENT = distance;

                    --low;
                    ++high;
                }

                // Trim the Histogram down to get rid of outliers that would make the chart useless.
                final htsjdk.samtools.util.Histogram<Integer> trimmedHisto = histogram; //alias it
                int actualWidth = inferHistogramWidth(HISTOGRAM_WIDTH, metrics, DEVIATIONS);

                trimmedHisto.trimByWidth(actualWidth);

                metrics.MEAN_INSERT_SIZE = trimmedHisto.getMean();
                metrics.STANDARD_DEVIATION = trimmedHisto.getStandardDeviation();

                metricsFile.addHistogram(trimmedHisto);
                metricsFile.addMetric(metrics);
            }
        }

        /**
         * If histogramWidth is null infer a value for it
         */
        private static int inferHistogramWidth(Integer histogramWidth, InsertSizeMetrics metrics, double deviations) {
            if (histogramWidth == null) {
                return (int) (metrics.MEDIAN_INSERT_SIZE + (deviations * metrics.MEDIAN_ABSOLUTE_DEVIATION));
            } else {
                return histogramWidth;
            }
        }

        @Override
        public CombineHistogramsIntoMetricsFile createAccumulator() {
            return new CombineHistogramsIntoMetricsFile(this.DEVIATIONS, this.HISTOGRAM_WIDTH, this.MINIMUM_PERCENT);
        }

        @Override
        public CombineHistogramsIntoMetricsFile addInput(CombineHistogramsIntoMetricsFile accumulator, KV<AggregationLevel, DataflowHistogram<Integer>> input) {
            accumulator.histograms.put(input.getKey(), input.getValue());
            return accumulator;
        }

        @Override
        public CombineHistogramsIntoMetricsFile mergeAccumulators(Iterable<CombineHistogramsIntoMetricsFile> accumulators) {
            Map<AggregationLevel, DataflowHistogram<Integer>> histograms = StreamSupport.stream(accumulators.spliterator(), false)
                    .flatMap(a -> a.histograms.entrySet().stream())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            CombineHistogramsIntoMetricsFile accum = createAccumulator();
            accum.histograms.putAll(histograms);
            return accum;
        }

        @Override
        public MetricsFileDataflow<InsertSizeMetrics, Integer> extractOutput(CombineHistogramsIntoMetricsFile accumulator) {
            final MetricsFileDataflow<InsertSizeMetrics,Integer> metricsFile = new MetricsFileDataflow<>();


            double totalInserts = accumulator.histograms.values()
                    .stream()
                    .mapToDouble(Histogram::getCount)
                    .sum();

            Map<AggregationLevel, DataflowHistogram<Integer>> sortedHistograms = new TreeMap<>(Comparator.comparing((AggregationLevel a) -> a.getSample() != null ? a.getSample(): "")
                .thenComparing(a -> a.getLibrary() != null ? a.getLibrary() : "")
                .thenComparing(a -> a.getReadGroup() != null ? a.getReadGroup() : ""));
            sortedHistograms.putAll(accumulator.histograms);
            sortedHistograms.entrySet().forEach(kv -> addHistogramToMetricsFile(kv.getKey(), kv.getValue(), metricsFile, totalInserts));
            return metricsFile;
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

    public final static class AggregationLevel extends GenericJson {
        @Key
        private final String orientation;
        @Key
        private final String readGroup;
        @Key
        private final String library;
        @Key
        private final String sample;

        public String getReadGroup() {
            return readGroup;
        }

        public String getLibrary() {
            return library;
        }

        public String getSample() {
            return sample;
        }


        public AggregationLevel(final SAMRecord read, final boolean includeLibrary, final boolean includeReadGroup, final boolean includeSample){
            this.orientation = SamPairUtil.getPairOrientation(read).toString();
            this.library = includeLibrary ? read.getReadGroup().getLibrary() : null;
            this.readGroup = includeReadGroup ? read.getReadGroup().getId() : null;
            this.sample = includeSample ? read.getReadGroup().getSample(): null;
        }

        public AggregationLevel(final String orientation, final String library, final String readgroup, final String sample) {
            this.orientation = orientation;
            this.library = library;
            this.readGroup = readgroup;
            this.sample = sample;
        }

        public static  List<AggregationLevel>getKeysForAllAggregationLevels(final SAMRecord read, final boolean includeAll, final boolean includeLibrary, final boolean includeReadGroup, final boolean includeSample){
            final List<AggregationLevel> aggregationLevels = new ArrayList<>();
            if(includeAll) {
                aggregationLevels.add(new AggregationLevel(read, false, false, false));
            }
            if(includeLibrary){
                aggregationLevels.add(new AggregationLevel(read,true, true, true));
            }
            if(includeReadGroup){
                aggregationLevels.add(new AggregationLevel(read, false, true, true));
            }
            if(includeSample){
                aggregationLevels.add(new AggregationLevel(read, false, false, true));
            }
            return aggregationLevels;

        }

        public SamPairUtil.PairOrientation getOrientation() {
            return SamPairUtil.PairOrientation.valueOf(orientation);
        }

        @Override
        public String toString() {
            return "AggregationLevel{" +
                    "orientation=" + orientation +
                    ", readGroup='" + readGroup + '\'' +
                    ", library='" + library + '\'' +
                    ", sample='" + sample + '\'' +
                    '}';
        }


    }

    public static class  CombineMetricsFiles<VI extends MetricsFileDataflow<BEAN,HKEY>, BEAN extends MetricBase & Serializable, HKEY extends Comparable<HKEY>>
    extends Combine.CombineFn<VI, MetricsFileDataflow<BEAN,HKEY>, MetricsFileDataflow<BEAN,HKEY>>{

        @Override
        public MetricsFileDataflow<BEAN,HKEY> createAccumulator() {
            return new MetricsFileDataflow<>();
        }

        @Override
        public MetricsFileDataflow<BEAN, HKEY> addInput(MetricsFileDataflow<BEAN, HKEY> accumulator, VI input) {
            return combineMetricsFiles(accumulator, input);
        }

        private MetricsFileDataflow<BEAN, HKEY> combineMetricsFiles(MetricsFileDataflow<BEAN, HKEY> accumulator, MetricsFileDataflow<BEAN,HKEY> input) {
            Set<Header> headers = Sets.newLinkedHashSet(accumulator.getHeaders());
            Set<Header> inputHeaders = Sets.newLinkedHashSet(input.getHeaders());
            inputHeaders.removeAll(headers);
            inputHeaders.stream().forEach(accumulator::addHeader);

            accumulator.addAllMetrics(input.getMetrics());
            input.getAllHistograms().stream().forEach(accumulator::addHistogram);
            return accumulator;
        }

        @Override
        public MetricsFileDataflow<BEAN, HKEY> mergeAccumulators(Iterable<MetricsFileDataflow<BEAN, HKEY>> accumulators) {
            MetricsFileDataflow<BEAN, HKEY> base = createAccumulator();
            accumulators.forEach(accum -> combineMetricsFiles(base,  accum));
            return base;
        }

        @Override
        public MetricsFileDataflow<BEAN, HKEY> extractOutput(MetricsFileDataflow<BEAN, HKEY> accumulator) {
            return accumulator;
        }

    }


}
