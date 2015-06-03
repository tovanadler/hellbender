package org.broadinstitute.hellbender.engine.dataflow.transforms.composite;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.collect.Lists;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReadContextData;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReferenceAPISource;
import org.broadinstitute.hellbender.engine.dataflow.datasources.VariantsFileSource;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.read.Read;
import org.broadinstitute.hellbender.utils.reference.ReferenceBases;
import org.broadinstitute.hellbender.utils.variant.Variant;

import java.util.List;
import java.util.UUID;


public class AddContextDataToRead {
    public static PCollection<KV<Read, ReadContextData>> Add(PCollection<Read> pReads, ReferenceAPISource referenceAPISource, VariantsFileSource variantsFileSource) {
        PCollection<Variant> pVariants = variantsFileSource.getAllVariants();
        PCollection<KV<Read, Iterable<Variant>>> kvReadVariants = KeyVariantsByRead.Key(pVariants, pReads);
        PCollection<KV<Read, ReferenceBases>> kvReadRefBases =
                APIToRefBasesKeyedByRead.addBases(pReads, referenceAPISource);
        return Join(pReads, kvReadRefBases, kvReadVariants);

    }

    protected static PCollection<KV<Read, ReadContextData>> Join(PCollection<Read> pReads, PCollection<KV<Read, ReferenceBases>> kvReadRefBases, PCollection<KV<Read, Iterable<Variant>>> kvReadVariants) {
        // We could add a check that all of the reads in kvReadRefBases, pVariants, and pReads are the same.
        PCollection<KV<UUID, Iterable<Variant>>> UUIDVariants = kvReadVariants.apply(ParDo.of(new DoFn<KV<Read, Iterable<Variant>>, KV<UUID, Iterable<Variant>>>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                Read r = c.element().getKey();
                c.output(KV.of(r.getUUID(), c.element().getValue()));
            }
        }));

        PCollection<KV<UUID, ReferenceBases>> UUIDRefBases = kvReadRefBases.apply(ParDo.of(new DoFn<KV<Read, ReferenceBases>, KV<UUID, ReferenceBases>>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                Read r = c.element().getKey();
                c.output(KV.of(r.getUUID(), c.element().getValue()));
            }
        }));

        final TupleTag<Iterable<Variant>> variantTag = new TupleTag<>();
        final TupleTag<ReferenceBases> referenceTag = new TupleTag<>();

        PCollection<KV<UUID, CoGbkResult>> coGbkInput = KeyedPCollectionTuple
                .of(variantTag, UUIDVariants)
                .and(referenceTag, UUIDRefBases).apply(CoGroupByKey.<UUID>create());

        PCollection<KV<UUID, ReadContextData>> UUIDcontext = coGbkInput.apply(ParDo.of(new DoFn<KV<UUID, CoGbkResult>, KV<UUID, ReadContextData>>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                Iterable<Iterable<Variant>> variants = c.element().getValue().getAll(variantTag);
                Iterable<ReferenceBases> referenceBases = c.element().getValue().getAll(referenceTag);

                // TODO: We should be enforcing that this is a singleton with getOnly somehow...
                List<Iterable<Variant>> vList = Lists.newArrayList();
                for (Iterable<Variant> v : variants) {
                    vList.add(v);
                }
                if (vList.size() != 1) {
                    throw new GATKException("expected one list of varints, got " + vList.size());
                }
                List<ReferenceBases> bList = Lists.newArrayList();
                for (ReferenceBases b : referenceBases) {
                    bList.add(b);
                }
                if (bList.size() != 1) {
                    throw new GATKException("expected one ReferenceBases, got " + bList.size());
                }

                c.output(KV.of(c.element().getKey(), new ReadContextData(bList.get(0), vList.get(0))));
            }
        }));

        // Now add the reads back in.
        PCollection<KV<UUID, Read>> UUIDRead = pReads.apply(ParDo.of(new DoFn<Read, KV<UUID, Read>>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                c.output(KV.of(c.element().getUUID(), c.element()));
            }
        }));
        final TupleTag<Read> readTag = new TupleTag<>();
        final TupleTag<ReadContextData> contextDataTag = new TupleTag<>();

        PCollection<KV<UUID, CoGbkResult>> coGbkfull = KeyedPCollectionTuple
                .of(readTag, UUIDRead)
                .and(contextDataTag, UUIDcontext).apply(CoGroupByKey.<UUID>create());

        return coGbkfull.apply(ParDo.of(new DoFn<KV<UUID, CoGbkResult>, KV<Read, ReadContextData>>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                Iterable<Read> reads = c.element().getValue().getAll(readTag);
                Iterable<ReadContextData> contextDatas = c.element().getValue().getAll(contextDataTag);

                // TODO: We should be enforcing that this is a singleton with getOnly somehow...
                List<Read> rList = Lists.newArrayList();
                for (Read r : reads) {
                    rList.add(r);
                }
                if (rList.size() != 1) {
                    throw new GATKException("expected one Read, got " + rList.size());
                }
                List<ReadContextData> cList = Lists.newArrayList();
                for (ReadContextData cd : contextDatas) {
                    cList.add(cd);
                }
                if (cList.size() != 1) {
                    throw new GATKException("expected one ReadContextData, got " + cList.size());
                }

                c.output(KV.of(rList.get(0), cList.get(0)));
            }
        }));
    }

}