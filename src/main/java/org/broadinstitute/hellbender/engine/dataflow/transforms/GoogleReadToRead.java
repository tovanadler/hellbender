package org.broadinstitute.hellbender.engine.dataflow.transforms;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.read.GoogleGenomicsReadToReadAdapter;
import org.broadinstitute.hellbender.utils.read.Read;

import java.util.UUID;

public class GoogleReadToRead extends PTransform<PCollection<KV<UUID, com.google.api.services.genomics.model.Read>>, PCollection<org.broadinstitute.hellbender.utils.read.Read>> {

    @Override
    public PCollection<Read> apply( PCollection<KV<UUID, com.google.api.services.genomics.model.Read>> input ) {
        return input.apply(ParDo.of(new DoFn<KV<UUID, com.google.api.services.genomics.model.Read>, org.broadinstitute.hellbender.utils.read.Read>() {
            @Override
            public void processElement( ProcessContext c ) throws Exception {
                c.output(new GoogleGenomicsReadToReadAdapter(c.element().getValue(), c.element().getKey()));
            }
        })).setCoder(GoogleGenomicsReadToReadAdapter.CODER);
    }
}
