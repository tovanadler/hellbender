package org.broadinstitute.hellbender.engine.dataflow.transforms;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.broadinstitute.hellbender.engine.dataflow.datasources.VariantShard;
import org.broadinstitute.hellbender.utils.read.Read;

import java.util.List;

public class KeyReadByVariantShard extends PTransform<PCollection<Read>, PCollection<KV<VariantShard, Read>>> {

    @Override
    public PCollection<KV<VariantShard, Read>> apply( PCollection<Read> input ) {
        return input.apply(ParDo.of(new DoFn<Read, KV<VariantShard, Read>>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                List<VariantShard> shards = VariantShard.getVariantShardsFromInterval(c.element());
                for (VariantShard shard : shards) {
                    c.output(KV.of(shard, c.element()));
                }
            }
        }).named("KeyReadByVariantShard"));
    }
}
