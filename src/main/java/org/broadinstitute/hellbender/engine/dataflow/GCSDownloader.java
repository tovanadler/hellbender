package org.broadinstitute.hellbender.engine.dataflow;

/**
 * Created by davidada on 5/7/15.
 */


        import com.google.api.client.auth.oauth2.Credential;
        import com.google.api.client.http.HttpTransport;
        import com.google.api.client.json.JsonFactory;
        import com.google.api.client.util.Preconditions;
        import com.google.api.services.storage.Storage;
        import com.google.api.services.storage.model.StorageObject;
        import com.google.cloud.dataflow.sdk.Pipeline;
        import com.google.cloud.dataflow.sdk.transforms.Create;
        import com.google.cloud.dataflow.sdk.transforms.DoFn;
        import com.google.cloud.dataflow.sdk.transforms.View;
        import com.google.cloud.dataflow.sdk.values.KV;
        import com.google.cloud.dataflow.sdk.values.PCollectionView;

        import java.io.File;
        import java.io.FileOutputStream;
        import java.io.IOException;
        import java.io.OutputStream;
        import java.util.Collections;
        import java.util.regex.Matcher;
        import java.util.regex.Pattern;


public class GCSDownloader {
    private static final Pattern GS_URL_REGEX =
            Pattern.compile("gs://((?:\\p{Lower}|\\p{Digit}|-|_|\\.)+)/(.+)");
    private Storage.Objects objects;

    private GCSDownloader(
            DoFn<?, ?>.Context context, WorkerCredentialFactory workerCredentialFactory) {
        GenomicsPipelineOptions options = GenomicsPipelineOptions.Methods.getFromContext(context);
        HttpTransport transport = options.getTransport();
        JsonFactory jsonFactory = options.getJsonFactory();
        Credential credential = workerCredentialFactory.getCredential(transport, jsonFactory);
        objects = new Storage.Builder(transport, jsonFactory, credential)
                .setApplicationName(options.getAppName())
                .build()
                .objects();
    }

    public static GCSDownloader of(
            DoFn<?, ?>.Context context, WorkerCredentialFactory workerCredentialFactory) {
        return new GCSDownloader(context, workerCredentialFactory);
    }

    public static PCollectionView<KV<String, String>> cachedFile(Pipeline pipeline,
                                                                    String local,
                                                                    String gcs) {
        return pipeline
                .apply(Create.of(Collections.singletonList(KV.of(local, gcs))))
                .apply(View.<KV<String, String>>asSingleton());
    }

    public File download(KV<String, String> kv) throws IOException {
        File file = new File(kv.getKey());
        if (!file.exists()) {
            Matcher matcher = GS_URL_REGEX.matcher(kv.getValue());
            Preconditions.checkArgument(matcher.matches());
            String bucket = matcher.group(1), name = matcher.group(2);
            Storage.Objects.Get request = objects.get(bucket, name);
            if ("gzip".equals(objects.get(bucket, name).execute().getContentEncoding())) {
                request.set("Accept-Encoding", "gzip");
            }
            try (OutputStream out = new FileOutputStream(file)) {
                request.executeMediaAndDownloadTo(out);
            }
        }
        return file;
    }

    public File download(StorageObject object) throws IOException {
        Storage.Objects.Get request = objects.get(object.getBucket(), object.getName());
        File file = File.createTempFile("gcsdownload", "obj");
        try (OutputStream out = new FileOutputStream(file)) {
            request.executeMediaAndDownloadTo(out);
        }
        return file;
    }
}