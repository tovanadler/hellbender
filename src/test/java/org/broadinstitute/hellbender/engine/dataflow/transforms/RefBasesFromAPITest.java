package org.broadinstitute.hellbender.engine.dataflow.transforms;

import com.google.appengine.repackaged.com.google.common.collect.Lists;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.readers.bam.ReadConverter;
import org.broadinstitute.hellbender.engine.dataflow.datasources.FakeReferenceSource;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReferenceAPISource;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReferenceShard;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.ArtificialSAMUtils;
import org.broadinstitute.hellbender.utils.read.GoogleGenomicsReadToReadAdapter;
import org.broadinstitute.hellbender.utils.read.Read;
import org.broadinstitute.hellbender.utils.reference.ReferenceBases;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public final class RefBasesFromAPITest {

    @DataProvider(name = "bases")
    public Object[][] bases(){
        List<KV<Integer, Integer>> rStartLength = Lists.newArrayList(KV.of(100, 10), KV.of(105, 10), KV.of(299999, 10));

        List<Read> inputs = Lists.newArrayList(makeRead(rStartLength.get(0), 1), makeRead(rStartLength.get(1), 2), makeRead(rStartLength.get(2), 3));

        List<KV<ReferenceShard, List<Read>>> kvs = Lists.newArrayList(
                KV.of(new ReferenceShard(0, "1"), Lists.newArrayList(inputs.get(1), inputs.get(0))),
                KV.of(new ReferenceShard(2, "1"), Lists.newArrayList(inputs.get(2))));

        // The first two reads are mapped onto the same reference shard. The ReferenceBases returned should
        // be from the start of the first read [rStartLength.get(0).getKey()] to the end
        // the second [rStartLength.get(1).getKey() + rStartLength.get(1).getValue()-1].
        SimpleInterval longerInterval = new SimpleInterval("1", rStartLength.get(0).getKey(), rStartLength.get(1).getKey() + rStartLength.get(1).getValue()-1);

        SimpleInterval shorterInterval = makeInterval(rStartLength.get(2));

        List<KV<ReferenceBases, Iterable<Read>>> expected = Lists.newArrayList(
                KV.of(FakeReferenceSource.bases(longerInterval), Lists.newArrayList(inputs.get(1), inputs.get(0))),
                KV.of(FakeReferenceSource.bases(shorterInterval), Lists.newArrayList(inputs.get(2)))
        );

        return new Object[][]{
                {inputs, kvs, expected, longerInterval, shorterInterval},
        };
    }

    private SimpleInterval makeInterval(KV<Integer, Integer> startLength) {
        return new SimpleInterval("1", startLength.getKey(), startLength.getKey() + startLength.getValue() - 1);
    }

    private Read makeRead(KV<Integer, Integer> startLength, int i) {
        return makeRead(startLength.getKey(), startLength.getValue(), i);
    }

    private Read makeRead(int start, int length, int i) {
        return new GoogleGenomicsReadToReadAdapter(ReadConverter.makeRead(ArtificialSAMUtils.createRandomRead(start, length)), new UUID(i, i));
    }

    @Test(dataProvider = "bases")
    public void refBasesTest(List<Read> reads, List<KV<ReferenceShard, Iterable<Read>>> kvs, List<KV<ReferenceBases, Iterable<Read>>> expected,
                           SimpleInterval longInterval, SimpleInterval shortInterval) {
        Pipeline p = TestPipeline.create();

        p.getCoderRegistry().registerCoder(ReferenceShard.class, ReferenceShard.CODER);
        p.getCoderRegistry().registerCoder(Read.class, GoogleGenomicsReadToReadAdapter.CODER);
        p.getCoderRegistry().registerCoder(GoogleGenomicsReadToReadAdapter.class, GoogleGenomicsReadToReadAdapter.CODER);

        ReferenceAPISource mockSource = mock(ReferenceAPISource.class, withSettings().serializable());
        when(mockSource.getReferenceBases(longInterval)).thenReturn(FakeReferenceSource.bases(longInterval));
        when(mockSource.getReferenceBases(shortInterval)).thenReturn(FakeReferenceSource.bases(shortInterval));

        List<Read> reads2 = Lists.newArrayList(reads.iterator());
        Assert.assertEquals(reads, reads2);
        PCollection<Read> pReads = p.apply(Create.of(reads));
        DataflowAssert.that(pReads).containsInAnyOrder(reads2);

        // We have to use GroupReads because if we try to create a pCollection that contains an ArrayList, we get a
        // coder error.
        PCollection<KV<ReferenceShard, Iterable<Read>>> pKvs = pReads.apply(new GroupReadsForRef());
        DataflowAssert.that(pKvs).containsInAnyOrder(kvs);

        PCollection<KV<ReferenceBases, Iterable<Read>>> kvpCollection = RefBasesFromAPI.GetBases(pKvs, mockSource);
        DataflowAssert.that(kvpCollection).containsInAnyOrder(expected);

        p.run();
    }
}
