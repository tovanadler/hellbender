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

public final class PairReadsAndVariantsTest {

    @DataProvider(name = "pairedReadsAndVariants")
    public Object[][] pairedReadsAndVariants(){
        List<Read> reads = Lists.newArrayList(makeRead(1, 300, 1), makeRead(100000, 10, 2), makeRead(299999, 10, 3));

        List<Variant> variantList = new ArrayList<>();
        variantList.add(new SkeletonVariant(new SimpleInterval("1", 10, 10), true, false, new UUID(1001, 1001)));
        variantList.add(new SkeletonVariant(new SimpleInterval("1", 199, 200), false, true, new UUID(1002, 1002)));
        variantList.add(new SkeletonVariant(new SimpleInterval("1", 100000, 100000), true, false, new UUID(1003, 1003)));
        variantList.add(new SkeletonVariant(new SimpleInterval("1", 299998, 300002), false, true, new UUID(1004, 1004)));

        List<KV<Read, Variant>> expected = Lists.newArrayList(
                KV.of(reads.get(0), variantList.get(0)),
                KV.of(reads.get(0), variantList.get(1)),
                KV.of(reads.get(1), variantList.get(2)),
                KV.of(reads.get(2), variantList.get(3)),    // The read and variant span two variant shards, that's
                KV.of(reads.get(2), variantList.get(3)));   // why there are two of them (2,3).

        return new Object[][]{
                {reads, variantList, expected},
        };
    }

    private Read makeRead(int start, int length, int i) {
        return new GoogleGenomicsReadToReadAdapter(ReadConverter.makeRead(ArtificialSAMUtils.createRandomRead(start, length)), new UUID(i, i));
    }

    @Test(dataProvider = "pairedReadsAndVariants")
    public void pairReadsAndVariantsTest(List<Read> reads, List<Variant> variantList, List<KV<Read, Variant>> expected) {
        Pipeline p = TestPipeline.create();
        p.getCoderRegistry().registerCoder(Read.class, GoogleGenomicsReadToReadAdapter.CODER);
        p.getCoderRegistry().registerCoder(GoogleGenomicsReadToReadAdapter.class, GoogleGenomicsReadToReadAdapter.CODER);

        p.getCoderRegistry().registerCoder(Variant.class, SerializableCoder.of(SkeletonVariant.class));
        p.getCoderRegistry().registerCoder(SkeletonVariant.class, SerializableCoder.of(SkeletonVariant.class));

        List<Read> reads2 = Lists.newArrayList(reads.iterator());
        Assert.assertEquals(reads, reads2);
        PCollection<Read> pReads = p.apply(Create.of(reads));
        DataflowAssert.that(pReads).containsInAnyOrder(reads2);

        // Create a PCollection of variants
        List<Variant> variantList1 = Lists.newArrayList(variantList.iterator());
        Assert.assertEquals(variantList, variantList1);
        PCollection<Variant> pVariants = p.apply(Create.of(variantList));
        DataflowAssert.that(pVariants).containsInAnyOrder(variantList1);

        PCollection<KV<Read, Variant>> readVariants = PairReadsAndVariants.Pair(pReads, pVariants);
        DataflowAssert.that(readVariants).containsInAnyOrder(expected);

        p.run();
    }
}