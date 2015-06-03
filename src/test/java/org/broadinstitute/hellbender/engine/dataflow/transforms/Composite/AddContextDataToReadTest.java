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
import org.broadinstitute.hellbender.engine.dataflow.datasources.*;
import org.broadinstitute.hellbender.engine.dataflow.transforms.RemoveDuplicatePairedReadVariants;
import org.broadinstitute.hellbender.engine.dataflow.transforms.UuidCoder;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.ArtificialSAMUtils;
import org.broadinstitute.hellbender.utils.read.GoogleGenomicsReadToReadAdapter;
import org.broadinstitute.hellbender.utils.read.Read;
import org.broadinstitute.hellbender.utils.reference.ReferenceBases;
import org.broadinstitute.hellbender.utils.variant.SkeletonVariant;
import org.broadinstitute.hellbender.utils.variant.Variant;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.*;

public final class AddContextDataToReadTest {

    @DataProvider(name = "bases")
    public Object[][] bases(){
        List<KV<Integer, Integer>> rStartLength = Lists.newArrayList(KV.of(1, 300), KV.of(100000, 10), KV.of(299999, 10));

        List<Read> reads = Lists.newArrayList(makeRead(rStartLength.get(0), 1), makeRead(rStartLength.get(1), 2), makeRead(rStartLength.get(2), 3));

        List<KV<Read, ReferenceBases>> kvs1 = Lists.newArrayList(
                KV.of(reads.get(0), getBases(reads.get(0).getStart(), reads.get(0).getEnd())),
                KV.of(reads.get(1), getBases(reads.get(1).getStart(), reads.get(1).getEnd())),
                KV.of(reads.get(2), getBases(reads.get(2).getStart(), reads.get(2).getEnd()))
        );

        List<SimpleInterval> intervals = Lists.newArrayList(makeInterval(rStartLength.get(0)), makeInterval(rStartLength.get(1)), makeInterval(rStartLength.get(2)));

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

        List<KV<Read, ReadContextData>> kvReadContextData = Lists.newArrayList(
                KV.of(kvs1.get(0).getKey(), new ReadContextData(kvs1.get(0).getValue(), kvReadiVariant.get(0).getValue())),
                KV.of(kvs1.get(1).getKey(), new ReadContextData(kvs1.get(1).getValue(), kvReadiVariant.get(1).getValue())),
                KV.of(kvs1.get(2).getKey(), new ReadContextData(kvs1.get(2).getValue(), kvReadiVariant.get(2).getValue()))
        );

        return new Object[][]{
                {reads, variantList, dupes, kvs1, kvReadContextData, intervals},
        };
    }

    private SimpleInterval makeInterval(KV<Integer, Integer> startLength) {
        return new SimpleInterval("1", startLength.getKey(), startLength.getKey() + startLength.getValue() - 1);
    }

    private ReferenceBases getBases(int start, int end) {
        return FakeReferenceSource.bases(new SimpleInterval("1", start, end));
    }

    private Read makeRead(KV<Integer, Integer> startLength, int i) {
        return makeRead(startLength.getKey(), startLength.getValue(), i);
    }

    private Read makeRead(int start, int length, int i) {
        return new GoogleGenomicsReadToReadAdapter(ReadConverter.makeRead(ArtificialSAMUtils.createRandomRead(start, length)), new UUID(i, i));
    }

    @Test(dataProvider = "bases")
    public void addContextDataTest(List<Read> reads, List<Variant> variantList, List<KV<Read, Variant>> dupes,
                                   List<KV<Read, ReferenceBases>> kvs1, List<KV<Read, ReadContextData>> kvReadContextData,
                                   List<SimpleInterval> intervals) {
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

        // We have to use RemoveDuplicatePairedReadVariants because if we try to create a pCollection that contains
        // an ArrayList, we get a coder error.
        PCollection<KV<Read, ReferenceBases>> pReadRef = p.apply(Create.of(kvs1));

        PCollection<KV<Read, Variant>> pKVs = p.apply(Create.of(dupes));
        PCollection<KV<Read, Iterable<Variant>>> pReadVariants = pKVs.apply(new RemoveDuplicatePairedReadVariants());

        PCollection<KV<Read, ReadContextData>> joinedResults = AddContextDataToRead.Join(pReads, pReadRef, pReadVariants);
        DataflowAssert.that(joinedResults).containsInAnyOrder(kvReadContextData);
        p.run();
    }

    @Test(dataProvider = "bases")
    public void fullTest(List<Read> reads, List<Variant> variantList, List<KV<Read, Variant>> dupes,
                      List<KV<Read, ReferenceBases>> kvs1, List<KV<Read, ReadContextData>> kvReadContextData,
                      List<SimpleInterval> intervals) {
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
        VariantsFileSource mockVariantsSource = mock(VariantsFileSource.class);

        when(mockVariantsSource.getAllVariants()).thenReturn(pVariant);

        ReferenceAPISource mockReferenceSource = mock(ReferenceAPISource.class, withSettings().serializable());
        for (SimpleInterval i : intervals) {
            when(mockReferenceSource.getReferenceBases(i)).thenReturn(FakeReferenceSource.bases(i));
        }

        PCollection<KV<Read, ReadContextData>> result = AddContextDataToRead.Add(pReads, mockReferenceSource, mockVariantsSource);
        DataflowAssert.that(result).containsInAnyOrder(kvReadContextData);
    }
}