package org.broadinstitute.hellbender.engine.dataflow.transforms;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.broadinstitute.hellbender.utils.read.Read;

import java.util.UUID;

public class SelfKeyReads extends PTransform<PCollection<Read>, PCollection<KV<UUID, Read>>> {
    @Override
    public PCollection<KV<UUID, Read>> apply(PCollection<Read> input) {
        return input.apply(ParDo.of(new DoFn<Read, KV<UUID, Read>>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                c.output(KV.of(c.element().getUUID(), c.element()));
            }
        }));
    }
}
