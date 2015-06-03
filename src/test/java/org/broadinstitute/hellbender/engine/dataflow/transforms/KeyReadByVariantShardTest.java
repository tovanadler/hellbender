package org.broadinstitute.hellbender.engine.dataflow.transforms;

import com.google.appengine.repackaged.com.google.common.collect.Lists;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.readers.bam.ReadConverter;
import org.broadinstitute.hellbender.engine.dataflow.datasources.VariantShard;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.ArtificialSAMUtils;
import org.broadinstitute.hellbender.utils.read.GoogleGenomicsReadToReadAdapter;
import org.broadinstitute.hellbender.utils.read.Read;
import org.broadinstitute.hellbender.utils.variant.SkeletonVariant;
import org.broadinstitute.hellbender.utils.variant.Variant;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public final class KeyReadByVariantShardTest {

    @DataProvider(name = "keyedVariantShardsReads")
    public Object[][] keyedVariantShardsReads(){
        List<Read> reads = Lists.newArrayList(makeRead(1, 300, 1), makeRead(100000, 10, 2), makeRead(299999, 10, 3));

        List<KV<VariantShard, Read>> expected = Lists.newArrayList(
                KV.of(new VariantShard(0, "1"), reads.get(0)),
                KV.of(new VariantShard(1, "1"), reads.get(1)),
                KV.of(new VariantShard(2, "1"), reads.get(2)),    // The last read spans
                KV.of(new VariantShard(3, "1"), reads.get(2)));   // two shards.

        return new Object[][]{
                {reads, expected},
        };
    }

    private Read makeRead(int start, int length, int i) {
        return new GoogleGenomicsReadToReadAdapter(ReadConverter.makeRead(ArtificialSAMUtils.createRandomRead(start, length)), new UUID(i, i));
    }

    @Test(dataProvider = "keyedVariantShardsReads")
    public void keyReadsByVariantShardTest(List<Read> readList, List<KV<VariantShard, Read>> expected) {
        Pipeline p = TestPipeline.create();
        p.getCoderRegistry().registerCoder(Read.class, GoogleGenomicsReadToReadAdapter.CODER);
        p.getCoderRegistry().registerCoder(GoogleGenomicsReadToReadAdapter.class, GoogleGenomicsReadToReadAdapter.CODER);

        List<Read> readList1 = Lists.newArrayList(readList.iterator());
        Assert.assertEquals(readList, readList1);
        PCollection<Read> pVariants = p.apply(Create.of(readList));
        DataflowAssert.that(pVariants).containsInAnyOrder(readList1);

        PCollection<KV<VariantShard, Read>> kVariant = pVariants.apply(new KeyReadByVariantShard());
        DataflowAssert.that(kVariant).containsInAnyOrder(expected);
        p.run();
    }
}