package org.broadinstitute.hellbender.utils.read;

import com.google.cloud.dataflow.sdk.coders.*;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import org.broadinstitute.hellbender.engine.dataflow.transforms.UuidCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

public class ReadCoder {

    /*
    public static final DelegateCoder<MutableRead, KV<GoogleGenomicsReadToReadAdapter, Boolean>> CODER =
            DelegateCoder.of(
                    KvCoder.of(GoogleGenomicsReadToReadAdapter.CODER, SerializableCoder.of(Boolean.class)),
                    new DelegateCoder.CodingFunction<MutableRead, KV<GoogleGenomicsReadToReadAdapter, Boolean>>() {
                        @Override
                        public KV<GoogleGenomicsReadToReadAdapter, Boolean> apply(MutableRead read) throws Exception {
                            if (read.getClass() == GoogleGenomicsReadToReadAdapter.class) {
                                return KV.of(((GoogleGenomicsReadToReadAdapter) read), true);
                            }
                            else {
                                 ((SAMRecordToReadAdapter) read).getSamRecord();

                            }
                        }
                    },
                    new DelegateCoder.CodingFunction<KV<UUID, com.google.api.services.genomics.model.Read>, Read>() {
                        @Override
                        public Read apply(KV<UUID, com.google.api.services.genomics.model.Read> kv) throws Exception {
                            return new GoogleGenomicsReadToReadAdapter(kv.getValue(), kv.getKey());
                        }
                    }
            );*/

    public static final CustomCoder<MutableRead> CODER = new CustomCoder<MutableRead>() {
        @Override
        public void encode(MutableRead value, OutputStream outStream, Context context) throws CoderException, IOException {
            Boolean isGoogleRead = value.getClass() == GoogleGenomicsReadToReadAdapter.class;
            SerializableCoder.of(Boolean.class).encode(isGoogleRead, outStream, context);

            if (isGoogleRead) {
                GoogleGenomicsReadToReadAdapter.CODER.encode(value, outStream, context);
            } else {
                SerializableCoder.of(SAMRecordToReadAdapter.class).encode(((SAMRecordToReadAdapter) value), outStream, context);
            }
        }

        @Override
        public MutableRead decode(InputStream inStream, Context context) throws CoderException, IOException {
            Boolean isGoogleRead = SerializableCoder.of(Boolean.class).decode(inStream, context);
            if (isGoogleRead) {
                return ((MutableRead) GoogleGenomicsReadToReadAdapter.CODER.decode(inStream, context));
            } else {
                return SerializableCoder.of(SAMRecordToReadAdapter.class).decode(inStream, context);
            }
        }
    };
}
