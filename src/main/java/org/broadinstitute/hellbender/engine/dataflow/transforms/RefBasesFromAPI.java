package org.broadinstitute.hellbender.engine.dataflow.transforms;

import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReferenceAPISource;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReferenceShard;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.Read;
import org.broadinstitute.hellbender.utils.reference.ReferenceBases;

public class RefBasesFromAPI {
    public static PCollection<KV<ReferenceBases, Iterable<Read>>> GetBases(PCollection<KV<ReferenceShard, Iterable<Read>>> reads, ReferenceAPISource source) {
        PCollectionView<ReferenceAPISource> sourceView = reads.getPipeline().apply(Create.of(source)).apply(View.<ReferenceAPISource>asSingleton());
        return reads.apply(ParDo.withSideInputs(sourceView).of(new DoFn<KV<ReferenceShard, Iterable<Read>>, KV<ReferenceBases, Iterable<Read>>>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                final ReferenceShard shard = c.element().getKey();
                final Iterable<Read> reads = c.element().getValue();
                int min = Integer.MAX_VALUE;
                int max = 1;
                for (Read r : reads) {
                    if (r.getStart() < min) {
                        min = r.getStart();
                    }
                    if (r.getEnd() > max) {
                        max = r.getEnd();
                    }
                }
                ReferenceBases bases = c.sideInput(sourceView).getReferenceBases(new SimpleInterval(shard.getContig(), min, max));
                c.output(KV.of(bases, reads));
            }
        }));
    }
}