package org.broadinstitute.hellbender.engine.dataflow.transforms;

import com.google.cloud.dataflow.sdk.coders.DelegateCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;

import java.util.UUID;

/**
 * This is basically StringDelgateCoder, except that we need to call fromString instead of a one arg constructor.
 */
public class UuidCoder {
    public static final DelegateCoder<UUID, String> CODER =
            DelegateCoder.of(
                StringUtf8Coder.of(),
                    new DelegateCoder.CodingFunction<UUID, String>() {
                        @Override
                        public String apply(UUID uuid) throws Exception {
                            return uuid.toString();
                        }
                    },
                    new DelegateCoder.CodingFunction<String, UUID>() {
                        @Override
                        public UUID apply(String s) throws Exception {
                            return UUID.fromString(s);
                        }
                    }
            );
}
