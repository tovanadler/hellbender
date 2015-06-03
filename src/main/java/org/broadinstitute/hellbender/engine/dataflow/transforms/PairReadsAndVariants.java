package org.broadinstitute.hellbender.engine.dataflow.transforms;

import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import org.broadinstitute.hellbender.engine.dataflow.datasources.VariantShard;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.Read;
import org.broadinstitute.hellbender.utils.variant.Variant;


public class PairReadsAndVariants {
    static public PCollection<KV<Read, Variant>> Pair(PCollection<Read> pRead, PCollection<Variant> pVariant) {

        PCollection<KV<VariantShard, Read>> vkReads = pRead.apply(new KeyReadByVariantShard());
        PCollection<KV<VariantShard, Variant>> vkVariants =
                pVariant.apply(new KeyVariantByVariantShard());

        // GroupBy VariantShard
        final TupleTag<Variant> variantTag = new TupleTag<>();
        final TupleTag<Read> readTag = new TupleTag<>();
        PCollection<KV<VariantShard, CoGbkResult>> coGbkInput = KeyedPCollectionTuple
                .of(variantTag, vkVariants)
                .and(readTag, vkReads).apply(CoGroupByKey.<VariantShard>create());

        // GroupBy Read
        return coGbkInput.apply(ParDo.of(
                new DoFn<KV<VariantShard, CoGbkResult>, KV<Read, Variant>>() {
                    @Override
                    public void processElement(ProcessContext c) throws Exception {
                        Iterable<Variant> kVariants = c.element().getValue().getAll(variantTag);
                        Iterable<Read> kReads = c.element().getValue().getAll(readTag);
                        // Compute overlap.
                        for (Read r : kReads) {
                            SimpleInterval readInterval = new SimpleInterval(r);
                            for (Variant v : kVariants) {
                                if (readInterval.overlaps(new SimpleInterval(v))) {
                                    c.output(KV.of(r, v));
                                }
                            }
                        }
                    }
                }));

    }
}
