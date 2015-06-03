package org.broadinstitute.hellbender.engine.dataflow.transforms;

import com.google.appengine.repackaged.com.google.common.collect.Lists;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.*;
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

import java.util.*;

public final class RemoveDuplicatePairedReadVariantsTest {

    @DataProvider(name = "dupedPairedReadsAndVariants")
    public Object[][] dupedPairedReadsAndVariants(){
        List<Read> reads = Lists.newArrayList(makeRead(1, 300, 1), makeRead(100000, 10, 2), makeRead(299999, 10, 3));

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

        List<KV<UUID, UUID>> kvUUIDUUID = Lists.newArrayList(
                KV.of(dupes.get(0).getKey().getUUID(), dupes.get(0).getValue().getUUID()),
                KV.of(dupes.get(1).getKey().getUUID(), dupes.get(1).getValue().getUUID()),
                KV.of(dupes.get(2).getKey().getUUID(), dupes.get(2).getValue().getUUID()),
                KV.of(dupes.get(3).getKey().getUUID(), dupes.get(3).getValue().getUUID()),
                KV.of(dupes.get(3).getKey().getUUID(), dupes.get(3).getValue().getUUID()));

        Iterable<UUID> uuids0 = Lists.newArrayList(dupes.get(0).getValue().getUUID(), dupes.get(1).getValue().getUUID());
        Iterable<UUID> uuids2 = Lists.newArrayList(dupes.get(2).getValue().getUUID());
        Iterable<UUID> uuids3 = Lists.newArrayList(dupes.get(3).getValue().getUUID());
        ArrayList<KV<UUID, Iterable<UUID>>> kvUUIDiUUID = Lists.newArrayList(
                KV.of(dupes.get(0).getKey().getUUID(), uuids0),
                KV.of(dupes.get(2).getKey().getUUID(), uuids2),
                KV.of(dupes.get(3).getKey().getUUID(), uuids3)
        );


        Iterable<Variant> variant01 = Lists.newArrayList(dupes.get(1).getValue(), dupes.get(0).getValue());
        Iterable<Variant> variant2 = Lists.newArrayList(dupes.get(2).getValue());
        Iterable<Variant> variant3 = Lists.newArrayList(dupes.get(3).getValue());
        List<KV<UUID, Iterable<Variant>>> kvUUIDiVariant = Lists.newArrayList(
                KV.of(dupes.get(0).getKey().getUUID(), variant01),
                KV.of(dupes.get(2).getKey().getUUID(), variant2),
                KV.of(dupes.get(3).getKey().getUUID(), variant3));

        List<KV<Read, Iterable<Variant>>> finalExpected = Lists.newArrayList(
                KV.of(dupes.get(0).getKey(), variant01),
                KV.of(dupes.get(2).getKey(), variant2),
                KV.of(dupes.get(3).getKey(), variant3));

        return new Object[][]{
                {dupes, kvUUIDUUID, kvUUIDiUUID, kvUUIDiVariant, finalExpected},
        };
    }

    private Read makeRead(int start, int length, int i) {
        return new GoogleGenomicsReadToReadAdapter(ReadConverter.makeRead(ArtificialSAMUtils.createRandomRead(start, length)), new UUID(i, i));
    }

    @Test(dataProvider = "dupedPairedReadsAndVariants")
    public void removeDupedReadVariantsTest(List<KV<Read, Variant>> dupes, List<KV<UUID, UUID>> kvUUIDUUID,
                                            ArrayList<KV<UUID, Iterable<UUID>>> kvUUIDiUUID, List<KV<UUID, Iterable<Variant>>> kvUUIDiVariant,
                                            List<KV<Read, Iterable<Variant>>> finalExpected) {
        Pipeline p = TestPipeline.create();
        p.getCoderRegistry().registerCoder(UUID.class, UuidCoder.CODER);
        p.getCoderRegistry().registerCoder(Read.class, GoogleGenomicsReadToReadAdapter.CODER);
        p.getCoderRegistry().registerCoder(GoogleGenomicsReadToReadAdapter.class, GoogleGenomicsReadToReadAdapter.CODER);

        p.getCoderRegistry().registerCoder(Variant.class, SerializableCoder.of(SkeletonVariant.class));
        p.getCoderRegistry().registerCoder(SkeletonVariant.class, SerializableCoder.of(SkeletonVariant.class));

        List<KV<Read, Variant>> dupes2 = Lists.newArrayList(dupes.iterator());
        Assert.assertEquals(dupes, dupes2);
        PCollection<KV<Read, Variant>> pKVs = p.apply(Create.of(dupes));
        DataflowAssert.that(pKVs).containsInAnyOrder(dupes2);

        PCollection<KV<UUID, UUID>> uuids = pKVs.apply(ParDo.of(new stripToUUIDs()));
        DataflowAssert.that(uuids).containsInAnyOrder(kvUUIDUUID);

        PCollection<KV<UUID, Iterable<UUID>>> readsIterableVariants = uuids.apply(RemoveDuplicates.<KV<UUID, UUID>>create()).apply(GroupByKey.<UUID, UUID>create());
        DataflowAssert.that(readsIterableVariants).containsInAnyOrder(kvUUIDiUUID);

        // Now add the variants back in.
        // Now join the stuff we care about back in.
        // Start by making KV<UUID, KV<UUID, Variant>> and joining that to KV<UUID, Iterable<UUID>>
        PCollection<KV<UUID, Iterable<Variant>>> matchedVariants = RemoveReadVariantDupesUtility.addBackVariants(pKVs, readsIterableVariants);
        DataflowAssert.that(matchedVariants).containsInAnyOrder(kvUUIDiVariant);

        PCollection<KV<Read, Iterable<Variant>>> finalResult = RemoveReadVariantDupesUtility.addBackReads(pKVs, matchedVariants);
        DataflowAssert.that(finalResult).containsInAnyOrder(finalExpected);

        p.run();
    }

    @Test(dataProvider = "dupedPairedReadsAndVariants")
    public void fullRemoveDupesTest(List<KV<Read, Variant>> dupes, List<KV<UUID, UUID>> kvUUIDUUID,
                                            ArrayList<KV<UUID, Iterable<UUID>>> kvUUIDiUUID, List<KV<UUID, Iterable<Variant>>> kvUUIDiVariant,
                                            List<KV<Read, Iterable<Variant>>> finalExpected) {
        Pipeline p = TestPipeline.create();
        p.getCoderRegistry().registerCoder(UUID.class, UuidCoder.CODER);
        p.getCoderRegistry().registerCoder(Read.class, GoogleGenomicsReadToReadAdapter.CODER);
        p.getCoderRegistry().registerCoder(GoogleGenomicsReadToReadAdapter.class, GoogleGenomicsReadToReadAdapter.CODER);

        p.getCoderRegistry().registerCoder(Variant.class, SerializableCoder.of(SkeletonVariant.class));
        p.getCoderRegistry().registerCoder(SkeletonVariant.class, SerializableCoder.of(SkeletonVariant.class));

        List<KV<Read, Variant>> dupes2 = Lists.newArrayList(dupes.iterator());
        Assert.assertEquals(dupes, dupes2);
        PCollection<KV<Read, Variant>> pKVs = p.apply(Create.of(dupes));
        DataflowAssert.that(pKVs).containsInAnyOrder(dupes2);

        PCollection<KV<Read, Iterable<Variant>>> result = pKVs.apply(new RemoveDuplicatePairedReadVariants());
        DataflowAssert.that(result).containsInAnyOrder(finalExpected);

        p.run();
    }
}