package org.broadinstitute.hellbender.engine.dataflow;

import com.google.appengine.repackaged.com.google.common.collect.Lists;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.broadinstitute.hellbender.engine.dataflow.transforms.UuidCoder;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.UUID;

public final class UuidCoderTest {

    @DataProvider(name = "uuids")
    public Object[][] uuids(){
        List<UUID> twoUuids = Lists.newArrayList(new UUID(1L, 1L), new UUID(2L, 2L));
        List<UUID> oneHundredUuids = Lists.newArrayList();
        for (int i = 0; i < 100; ++i) {
            oneHundredUuids.add(UUID.randomUUID());
        }

        return new Object[][]{
                {twoUuids},
                {oneHundredUuids},
        };
    }

    @Test(dataProvider = "uuids")
    public void createUuidsTest(List<UUID> uuids) {
        // The simplest way to figure out if a class is coded correctly is to create a PCollection
        // of that type and see if matches the List version.
        Pipeline p = TestPipeline.create();
        p.getCoderRegistry().registerCoder(UUID.class, UuidCoder.CODER);

        PCollection<UUID> pShards = p.apply(Create.of(uuids));

        List<UUID> sameUuids = Lists.newArrayList();
        Assert.assertTrue(sameUuids.addAll(uuids));
        DataflowAssert.that(pShards).containsInAnyOrder(sameUuids);
        p.run();
    }

    @Test(dataProvider = "uuids")
    public void uniqueUuidsTest(List<UUID> uuids) {
        // Note that this is a probabilistic test. It's possible that this test could fail by chance,
        // but the test is designed to fail with fewer than a one in a million attempts (easily).
        Pipeline p = TestPipeline.create();
        p.getCoderRegistry().registerCoder(UUID.class, UuidCoder.CODER);

        PCollection<UUID> pShards = p.apply(Create.of(uuids));
        // createUuidsTest makes sure the UUIDs are coded correctly, we assume they are here.

        // Count the number of unique UUIDs and make sure that count matches the total number of UUIDs.
        PCollection<Long> counts = pShards.apply(RemoveDuplicates.<UUID>create()).apply(Count.<UUID>globally());
        List<Long> expectedCounts = Lists.newArrayList((long) uuids.size());
        DataflowAssert.that(counts).containsInAnyOrder(expectedCounts);
        p.run();
    }
}
