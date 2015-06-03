package org.broadinstitute.hellbender.engine.dataflow.transforms;

import com.google.appengine.repackaged.com.google.common.collect.Lists;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.readers.bam.ReadConverter;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReferenceShard;
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

public final class KeyVariantByVariantShardTest {

    @DataProvider(name = "keyedVariantShards")
    public Object[][] keyedVariantShards(){
        List<Variant> variantList = new ArrayList<>();
        variantList.add(new SkeletonVariant(new SimpleInterval("1", 100, 100), true, false, new UUID(1001, 1001)));
        variantList.add(new SkeletonVariant(new SimpleInterval("1", 199, 200), false, true, new UUID(1002, 1002)));
        variantList.add(new SkeletonVariant(new SimpleInterval("1", 100000, 100000), true, false, new UUID(1003, 1003)));
        variantList.add(new SkeletonVariant(new SimpleInterval("1", 299999, 300000), false, true, new UUID(1004, 1004)));

        List<KV<VariantShard, Variant>> expected = Lists.newArrayList(
                KV.of(new VariantShard(0, "1"), variantList.get(0)),
                KV.of(new VariantShard(0, "1"), variantList.get(1)),
                KV.of(new VariantShard(1, "1"), variantList.get(2)),
                KV.of(new VariantShard(2, "1"), variantList.get(3)),    // The last variant spans
                KV.of(new VariantShard(3, "1"), variantList.get(3)));   // two shards.

        return new Object[][]{
                {variantList, expected},
        };
    }

    @Test(dataProvider = "keyedVariantShards")
    public void keyVariantsByVariantShardTest(List<Variant> variantList, List<KV<VariantShard, Variant>> expected) {
        Pipeline p = TestPipeline.create();
        p.getCoderRegistry().registerCoder(Variant.class, SerializableCoder.of(SkeletonVariant.class));
        p.getCoderRegistry().registerCoder(SkeletonVariant.class, SerializableCoder.of(SkeletonVariant.class));

        List<Variant> variantList1 = Lists.newArrayList(variantList.iterator());
        Assert.assertEquals(variantList, variantList1);
        PCollection<Variant> pVariants = p.apply(Create.of(variantList));
        DataflowAssert.that(pVariants).containsInAnyOrder(variantList1);

        PCollection<KV<VariantShard, Variant>> kVariant = pVariants.apply(new KeyVariantByVariantShard());
        DataflowAssert.that(kVariant).containsInAnyOrder(expected);
        p.run();
    }
}