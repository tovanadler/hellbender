package org.broadinstitute.hellbender.engine.dataflow.reference;

/*

...
 */
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import org.broadinstitute.hellbender.engine.dataflow.GCSDownloader;
import org.broadinstitute.hellbender.engine.dataflow.WorkerCredentialFactory;

import java.util.Map;

public abstract class DoFnWithReference<I, O> extends DoFn<I, O> {

    private Map<String, FastaReader.Contig> contigs;
    private final KV<String, String> fai;
    private final KV<String, String> fasta;
    private FastaReader reader;
    private final WorkerCredentialFactory workerCredentialFactory;

    protected DoFnWithReference(
            WorkerCredentialFactory workerCredentialFactory,
            KV<String, String> fasta,
            KV<String, String> fai) {
        this.workerCredentialFactory = workerCredentialFactory;
        this.fasta = fasta;
        this.fai = fai;
    }

    @Override
    public final void finishBundle(Context context) throws Exception {
        finishBundleImpl(context, contigs);
        reader.close();
    }

    public void finishBundleImpl(Context context, Map<String, FastaReader.Contig> contigs)
            throws Exception {
    }

    @Override
    public final void processElement(ProcessContext context) throws Exception {
        processElementImpl(context, contigs);
    }

    public abstract void processElementImpl(ProcessContext context,
                                            Map<String, FastaReader.Contig> contigs) throws Exception;

    @Override
    public final void startBundle(Context context) throws Exception {
        GCSDownloader gcs = GCSDownloader.of(context, workerCredentialFactory);
        reader = FastaReader.create(
                gcs.download(fasta),
                gcs.download(fai));
        startBundleImpl(context, contigs = reader.contigs());
    }

    public void startBundleImpl(Context context, Map<String, FastaReader.Contig> contigs)
            throws Exception {
    }
}