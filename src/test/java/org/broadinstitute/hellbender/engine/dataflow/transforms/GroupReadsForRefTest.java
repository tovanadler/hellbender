package org.broadinstitute.hellbender.engine.dataflow.transforms;

import com.google.appengine.repackaged.com.google.common.collect.Lists;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.dataflow.readers.bam.ReadConverter;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReferenceShard;
import org.broadinstitute.hellbender.utils.read.ArtificialSAMUtils;
import org.broadinstitute.hellbender.utils.read.GoogleGenomicsReadToReadAdapter;
import org.broadinstitute.hellbender.utils.read.Read;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;

public final class GroupReadsForRefTest {

    @DataProvider(name = "refShards")
    public Object[][] refShards(){

        List<Read> inputs = Lists.newArrayList(makeRead(100, 10, 1), makeRead(200, 10, 2), makeRead(100000, 10, 3), makeRead(299999, 10, 3));
        List<KV<ReferenceShard, List<Read>>> kvs = Lists.newArrayList(
                KV.of(new ReferenceShard(0, "1"), Lists.newArrayList(inputs.get(1), inputs.get(0))),
                KV.of(new ReferenceShard(1, "1"), Lists.newArrayList(inputs.get(2))),
                KV.of(new ReferenceShard(2, "1"), Lists.newArrayList(inputs.get(3)))
                );
        return new Object[][]{
                {inputs, kvs},
        };
    }

    private Read makeRead(int start, int length, int i) {
        return new GoogleGenomicsReadToReadAdapter(ReadConverter.makeRead(ArtificialSAMUtils.createRandomRead(start, length)), new UUID(i, i));
    }

    @Test(dataProvider = "refShards")
    public void groupReadsForRefTest(List<Read> reads, List<KV<ReferenceShard, Iterable<Read>>> expectedResult) {
        Pipeline p = TestPipeline.create();
        p.getCoderRegistry().registerCoder(ReferenceShard.class, ReferenceShard.CODER);
        p.getCoderRegistry().registerCoder(Read.class, GoogleGenomicsReadToReadAdapter.CODER);
        p.getCoderRegistry().registerCoder(GoogleGenomicsReadToReadAdapter.class, GoogleGenomicsReadToReadAdapter.CODER);

        List<Read> reads2 = Lists.newArrayList(reads.iterator());
        Assert.assertEquals(reads, reads2);
        PCollection<Read> pReads = p.apply(Create.of(reads));
        DataflowAssert.that(pReads).containsInAnyOrder(reads2);
        PCollection<KV<ReferenceShard, Iterable<Read>>> grouped = pReads.apply(new GroupReadsForRef());

        DataflowAssert.that(grouped).containsInAnyOrder(expectedResult);
        p.run();
    }
}