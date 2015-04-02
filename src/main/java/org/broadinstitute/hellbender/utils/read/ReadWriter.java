package org.broadinstitute.hellbender.utils.read;

import java.io.Closeable;

public interface ReadWriter extends Closeable {
    void addRead(final Read read);
}
