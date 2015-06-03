package org.broadinstitute.hellbender.engine.dataflow.transforms;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReferenceShard;
import org.broadinstitute.hellbender.utils.read.Read;

public class GroupReadsForRef extends PTransform<PCollection<Read>, PCollection<KV<ReferenceShard, Iterable<Read>>>> {
    @Override
    public PCollection<KV<ReferenceShard, Iterable<Read>>> apply(PCollection<Read> input) {
        PCollection<KV<ReferenceShard, Read>> keyReadByReferenceShard = input.apply(ParDo.of(new DoFn<Read, KV<ReferenceShard, Read>>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                ReferenceShard shard = ReferenceShard.getShardNumberFromInterval(c.element());
                c.output(KV.of(shard, c.element()));
            }
        }).named("KeyReadByReferenceShard"));
        return keyReadByReferenceShard.apply(GroupByKey.<ReferenceShard, Read>create());
    }
}

