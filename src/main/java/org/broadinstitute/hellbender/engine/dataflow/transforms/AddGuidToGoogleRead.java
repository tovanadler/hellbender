package org.broadinstitute.hellbender.engine.dataflow.transforms;

import com.google.api.services.genomics.model.Read;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;

import java.util.UUID;

/**
 * Created by davidada on 6/2/15.
 */
public class AddGuidToGoogleRead extends PTransform<PCollection<Read>, PCollection<KV<UUID, Read>>> {

    @Override
    public PCollection<KV<UUID, Read>> apply( PCollection<Read> input ) {
        return input.apply(ParDo.of(new DoFn<Read, KV<UUID, Read>>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                c.output(KV.of(UUID.randomUUID(), c.element()));
            }
        })); //.setCoder(KvCoder.of(UuidCoder.CODER, GenericJsonCoder.of(Read.class)));
    }
}
