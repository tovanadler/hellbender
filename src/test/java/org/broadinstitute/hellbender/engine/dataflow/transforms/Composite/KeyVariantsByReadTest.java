package org.broadinstitute.hellbender.engine.dataflow.transforms.composite;

import com.google.appengine.repackaged.com.google.common.collect.Lists;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.readers.bam.ReadConverter;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReferenceShard;
import org.broadinstitute.hellbender.engine.dataflow.transforms.UuidCoder;
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

public final class KeyVariantsByReadTest {

    @DataProvider(name = "variantsAndReads")
    public Object[][] variantsAndReads(){
        List<KV<Integer, Integer>> rStartLength = Lists.newArrayList(KV.of(1, 300), KV.of(100000, 10), KV.of(299999, 10));

        List<Read> reads = Lists.newArrayList(makeRead(rStartLength.get(0), 1), makeRead(rStartLength.get(1), 2), makeRead(rStartLength.get(2), 3));

        List<Variant> variantList = new ArrayList<>();
        variantList.add(new SkeletonVariant(new SimpleInterval("1", 10, 10), true, false, new UUID(1001, 1001)));
        variantList.add(new SkeletonVariant(new SimpleInterval("1", 199, 200), false, true, new UUID(1002, 1002)));
        variantList.add(new SkeletonVariant(new SimpleInterval("1", 100000, 100000), true, false, new UUID(1003, 1003)));
        variantList.add(new SkeletonVariant(new SimpleInterval("1", 299998, 300002), false, true, new UUID(1004, 1004)));

        List<KV<Read, Variant>> dupes = Lists.newArrayList(
                KV.of(reads.get(0), variantList.get(0)),
                KV.of(reads.get(0), variantList.get(1)),
                KV.of(reads.get(1), variantList.get(2)),
                KV.of(reads.get(2), variantList.get(3)),    // The read and variant span two variant shards, that's
                KV.of(reads.get(2), variantList.get(3)));   // why there are two of them (2,3).

        Iterable<Variant> variant01 = Lists.newArrayList(dupes.get(1).getValue(), dupes.get(0).getValue());
        Iterable<Variant> variant2 = Lists.newArrayList(dupes.get(2).getValue());
        Iterable<Variant> variant3 = Lists.newArrayList(dupes.get(3).getValue());

        List<KV<Read, Iterable<Variant>>> kvReadiVariant = Lists.newArrayList(
                KV.of(dupes.get(0).getKey(), variant01),
                KV.of(dupes.get(2).getKey(), variant2),
                KV.of(dupes.get(3).getKey(), variant3));

        return new Object[][]{
                {reads, variantList, kvReadiVariant},
        };
    }

    private Read makeRead(KV<Integer, Integer> startLength, int i) {
        return makeRead(startLength.getKey(), startLength.getValue(), i);
    }

    private Read makeRead(int start, int length, int i) {
        return new GoogleGenomicsReadToReadAdapter(ReadConverter.makeRead(ArtificialSAMUtils.createRandomRead(start, length)), new UUID(i, i));
    }

    @Test(dataProvider = "variantsAndReads")
    public void addContextDataTest(List<Read> reads, List<Variant> variantList,
                                   List<KV<Read, Iterable<Variant>>> kvReadiVariant) {
        Pipeline p = TestPipeline.create();
        p.getCoderRegistry().registerCoder(UUID.class, UuidCoder.CODER);
        p.getCoderRegistry().registerCoder(ReferenceShard.class, ReferenceShard.CODER);
        p.getCoderRegistry().registerCoder(Read.class, GoogleGenomicsReadToReadAdapter.CODER);
        p.getCoderRegistry().registerCoder(GoogleGenomicsReadToReadAdapter.class, GoogleGenomicsReadToReadAdapter.CODER);
        p.getCoderRegistry().registerCoder(Variant.class, SerializableCoder.of(SkeletonVariant.class));
        p.getCoderRegistry().registerCoder(SkeletonVariant.class, SerializableCoder.of(SkeletonVariant.class));

        List<Read> reads2 = Lists.newArrayList(reads.iterator());
        Assert.assertEquals(reads, reads2);
        PCollection<Read> pReads = p.apply(Create.of(reads));
        DataflowAssert.that(pReads).containsInAnyOrder(reads2);

        PCollection<Variant> pVariant = p.apply(Create.of(variantList));

        PCollection<KV<Read, Iterable<Variant>>> result = KeyVariantsByRead.Key(pVariant, pReads);
        DataflowAssert.that(result).containsInAnyOrder(kvReadiVariant);
        p.run();
    }
}