package org.broadinstitute.hellbender.engine.dataflow.transforms.composite;

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
import org.broadinstitute.hellbender.engine.dataflow.transforms.composite.APIToRefBasesKeyedByRead;
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public final class APIToRefBasesKeyedByReadTest {
    @DataProvider(name = "bases")
    public Object[][] bases(){
        List<KV<Integer, Integer>> rStartLength = Lists.newArrayList(KV.of(1, 300), KV.of(100000, 10), KV.of(299999, 10));

        List<Read> reads = Lists.newArrayList(makeRead(rStartLength.get(0), 1), makeRead(rStartLength.get(1), 2), makeRead(rStartLength.get(2), 3));

        List<SimpleInterval> intervals = Lists.newArrayList(makeInterval(rStartLength.get(0)), makeInterval(rStartLength.get(1)), makeInterval(rStartLength.get(2)));

        List<KV<Read, ReferenceBases>> kvs1 = Lists.newArrayList(
                KV.of(reads.get(0), getBases(reads.get(0).getStart(), reads.get(0).getEnd())),
                KV.of(reads.get(1), getBases(reads.get(1).getStart(), reads.get(1).getEnd())),
                KV.of(reads.get(2), getBases(reads.get(2).getStart(), reads.get(2).getEnd()))
        );

        return new Object[][]{
                {reads, kvs1, intervals},
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
    public void refBasesTest(List<Read> reads, List<KV<Read, ReferenceBases>> kvs1,
                             List<SimpleInterval> intervals) {
        Pipeline p = TestPipeline.create();
        p.getCoderRegistry().registerCoder(ReferenceShard.class, ReferenceShard.CODER);
        p.getCoderRegistry().registerCoder(Read.class, GoogleGenomicsReadToReadAdapter.CODER);
        p.getCoderRegistry().registerCoder(GoogleGenomicsReadToReadAdapter.class, GoogleGenomicsReadToReadAdapter.CODER);


        ReferenceAPISource mockSource = mock(ReferenceAPISource.class, withSettings().serializable());
        for (SimpleInterval i : intervals) {
            when(mockSource.getReferenceBases(i)).thenReturn(FakeReferenceSource.bases(i));
        }

        List<Read> reads2 = Lists.newArrayList(reads.iterator());
        Assert.assertEquals(reads, reads2);
        PCollection<Read> pReads = p.apply(Create.of(reads));
        DataflowAssert.that(pReads).containsInAnyOrder(reads2);

        PCollection<KV<Read, ReferenceBases>> result = APIToRefBasesKeyedByRead.addBases(pReads, mockSource);
        DataflowAssert.that(result).containsInAnyOrder(kvs1);
        p.run();
    }

}