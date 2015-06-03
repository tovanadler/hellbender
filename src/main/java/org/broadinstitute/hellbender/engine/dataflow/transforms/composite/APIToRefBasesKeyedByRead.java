package org.broadinstitute.hellbender.engine.dataflow.transforms.composite;

import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReferenceAPISource;
import org.broadinstitute.hellbender.engine.dataflow.transforms.GroupReadsForRef;
import org.broadinstitute.hellbender.engine.dataflow.transforms.RefBasesFromAPI;
import org.broadinstitute.hellbender.engine.dataflow.transforms.GroupReadWithRefBases;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReferenceShard;
import org.broadinstitute.hellbender.utils.read.Read;
import org.broadinstitute.hellbender.utils.reference.ReferenceBases;


public class APIToRefBasesKeyedByRead {
    public static PCollection<KV<Read, ReferenceBases>> addBases(PCollection<Read> pReads, ReferenceAPISource refBasesSource) {
        PCollection<KV<ReferenceShard, Iterable<Read>>> shardAndRead = pReads.apply(new GroupReadsForRef());
        PCollection<KV<ReferenceBases, Iterable<Read>>> GroupedReads =
                RefBasesFromAPI.GetBases(shardAndRead, refBasesSource);
        return GroupedReads.apply(new GroupReadWithRefBases());
    }
}
