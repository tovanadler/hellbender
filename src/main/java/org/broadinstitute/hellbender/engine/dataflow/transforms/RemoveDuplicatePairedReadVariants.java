package org.broadinstitute.hellbender.engine.dataflow.transforms;

import com.google.api.client.util.Sets;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.*;
import com.google.common.collect.Lists;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.read.Read;
import org.broadinstitute.hellbender.utils.variant.Variant;

import java.util.List;
import java.util.Set;
import java.util.UUID;


public class RemoveDuplicatePairedReadVariants extends PTransform<PCollection<KV<Read,Variant>>, PCollection<KV<Read, Iterable<Variant>>>> {

    @Override
    public PCollection<KV<Read, Iterable<Variant>>> apply(PCollection<KV<Read, Variant>> input) {
        PCollection<KV<UUID, UUID>> readVariantUuids = input.apply(ParDo.of(new stripToUUIDs()));

        PCollection<KV<UUID, Iterable<UUID>>> readsIterableVariants = readVariantUuids.apply(RemoveDuplicates.<KV<UUID, UUID>>create()).apply(GroupByKey.<UUID, UUID>create());

        // Now join the stuff we care about back in.
        // Start by making KV<UUID, KV<UUID, Variant>> and joining that to KV<UUID, Iterable<UUID>>
        PCollection<KV<UUID, Iterable<Variant>>> matchedVariants =
                RemoveReadVariantDupesUtility.addBackVariants(input, readsIterableVariants);

        // And now, we do the same song and dance to get the Reads back in.
        return RemoveReadVariantDupesUtility.addBackReads(input, matchedVariants);
    }
}

class stripToUUIDs extends DoFn<KV<Read, Variant>, KV<UUID, UUID>> {

    @Override
    public void processElement(ProcessContext c) throws Exception {
        c.output(KV.of(c.element().getKey().getUUID(), c.element().getValue().getUUID()));
    }
}

class RemoveReadVariantDupesUtility {
    public static PCollection<KV<UUID, Iterable<Variant>>> addBackVariants(PCollection<KV<Read, Variant>> readVariants, PCollection<KV<UUID, Iterable<UUID>>> readsIterableVariants) {
        // Now join the stuff we care about back in.
        // Start by making KV<UUID, KV<UUID, Variant>> and joining that to KV<UUID, Iterable<UUID>>
        PCollection<KV<UUID, KV<UUID, Variant>>> variantToBeJoined =
                readVariants.apply(ParDo.of(new DoFn<KV<Read, Variant>, KV<UUID, KV<UUID, Variant>>>() {
                    @Override
                    public void processElement(ProcessContext c) throws Exception {
                        c.output(KV.of(c.element().getKey().getUUID(), KV.of(c.element().getValue().getUUID(), c.element().getValue())));
                    }
                }));

        // Another coGroupBy...
        final TupleTag<Iterable<UUID>> iterableUuidTag = new TupleTag<>();
        final TupleTag<KV<UUID, Variant>> variantUuidTag = new TupleTag<>();

        PCollection<KV<UUID, CoGbkResult>> coGbkAgain = KeyedPCollectionTuple
                .of(iterableUuidTag, readsIterableVariants)
                .and(variantUuidTag, variantToBeJoined).apply(CoGroupByKey.<UUID>create());

        // For each Read UUID, get all of the variants that have a variant UUID
        return coGbkAgain.apply(ParDo.of(
                new DoFn<KV<UUID, CoGbkResult>, KV<UUID, Iterable<Variant>>>() {
                    @Override
                    public void processElement(ProcessContext c) throws Exception {
                        Iterable<KV<UUID,Variant>> kVariants = c.element().getValue().getAll(variantUuidTag);
                        Iterable<Iterable<UUID>> kUUIDss = c.element().getValue().getAll(iterableUuidTag);
                        // For every UUID that's left, keep the variant.
                        Set<UUID> uuidHashSet = Sets.newHashSet();
                        for (Iterable<UUID> uuids : kUUIDss) {
                            for (UUID uuid : uuids) {
                                uuidHashSet.add(uuid);
                            }
                        }
                        Set<Variant> iVariants = Sets.newHashSet();
                        for (KV<UUID,Variant> uVariants : kVariants) {
                            if (uuidHashSet.contains(uVariants.getKey())) {
                                iVariants.add(uVariants.getValue());
                            }
                        }
                        c.output(KV.of(c.element().getKey(), iVariants));
                    }
                }));

    }

    public static PCollection<KV<Read, Iterable<Variant>>> addBackReads(PCollection<KV<Read, Variant>> readVariants, PCollection<KV<UUID, Iterable<Variant>>> matchedVariants) {
        // And now, we do the same song and dance to get the Reads back in.
        final TupleTag<Read> justReadTag = new TupleTag<>();
        final TupleTag<Iterable<Variant>> iterableVariant = new TupleTag<>();

        PCollection<KV<UUID, Read>> kReads = readVariants.apply(Keys.<Read>create()).apply(new SelfKeyReads());
        PCollection<KV<UUID, CoGbkResult>> coGbkLast = KeyedPCollectionTuple
                .of(justReadTag, kReads)
                .and(iterableVariant, matchedVariants).apply(CoGroupByKey.<UUID>create());

        return coGbkLast.apply(ParDo.of(new DoFn<KV<UUID, CoGbkResult>, KV<Read, Iterable<Variant>>>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                Iterable<Read> iReads = c.element().getValue().getAll(justReadTag);
                // We only care about the first read (the rest are the same.
                Iterable<Iterable<Variant>> variants = c.element().getValue().getAll(iterableVariant);
                List<Read> reads = Lists.newArrayList();
                for (Read r : iReads) {
                    reads.add(r);
                }
                if (reads.size() < 1) {
                    throw new GATKException("no read found");
                }

                for (Iterable<Variant> v : variants) {
                        c.output(KV.of(reads.get(0), v));
                }
            }
        }));
    }
}
