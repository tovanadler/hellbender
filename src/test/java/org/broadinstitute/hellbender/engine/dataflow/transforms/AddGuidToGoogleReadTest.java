package org.broadinstitute.hellbender.engine.dataflow.transforms;

import com.google.api.services.genomics.model.Read;
import com.google.appengine.repackaged.com.google.common.collect.Lists;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.readers.bam.ReadConverter;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReferenceShard;
import org.broadinstitute.hellbender.utils.read.ArtificialSAMUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.UUID;

public final class AddGuidToGoogleReadTest {

    @DataProvider(name = "readWithGuids")
    public Object[][] readWithGuids(){

        Read[] twoInputs = new Read[]{makeRead(100), makeRead(200)};
        return new Object[][]{
                {twoInputs, 2L},
        };
    }

    private Read makeRead(int i) {
        return ReadConverter.makeRead(ArtificialSAMUtils.createRandomRead(i));
    }

    @Test(dataProvider = "readWithGuids")
    public void groupReadsForRefTest(Read[] inputs, Long total) {
        Pipeline p = TestPipeline.create();
        DataflowWorkarounds.registerGenomicsCoders(p); // Gets the Google read coder.
        p.getCoderRegistry().registerCoder(UUID.class, UuidCoder.CODER);

        List<Read> reads = Lists.newArrayList(inputs);
        List<Read> reads2 = Lists.newArrayList(inputs);
        Assert.assertEquals(reads, reads2);
        PCollection<Read> pReads = p.apply(Create.of(reads));
        DataflowAssert.that(pReads).containsInAnyOrder(reads2);
        PCollection<KV<UUID, Read>> readsWithGUID = pReads.apply(new AddGuidToGoogleRead());
        // We want to check that the GUIDs are indeed unique. We just get the keys, remove duplicates, then count.
        PCollection<Long> counts = readsWithGUID.apply(
                Keys.<UUID>create()).apply(
                RemoveDuplicates.<UUID>create()).apply(
                Count.<UUID>globally());
        List<Long> expectedCounts = Lists.newArrayList(total);
        DataflowAssert.that(counts).containsInAnyOrder(expectedCounts);

        p.run();
    }
}