package org.broadinstitute.hellbender.engine.dataflow;

/**
 * Created by davidada on 5/7/15.
 */
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.java6.auth.oauth2.VerificationCodeReceiver;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.dataflow.DataflowScopes;
import com.google.api.services.genomics.GenomicsScopes;
import com.google.api.services.storage.StorageScopes;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.List;

public interface GenomicsPipelineOptions extends DataflowPipelineOptions {

    class DataStoreFactoryFactory implements DefaultValueFactory<DataStoreFactory> {
        @Override
        public DataStoreFactory create(PipelineOptions options) {
            try {
                return new FileDataStoreFactory(new File(options.as(GcpOptions.class).getCredentialDir()));
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    class HttpTransportFactory implements DefaultValueFactory<HttpTransport> {
        @Override
        public HttpTransport create(PipelineOptions options) {
            return Utils.getDefaultTransport();
        }
    }

    class JsonFactoryFactory implements DefaultValueFactory<JsonFactory> {
        @Override
        public JsonFactory create(PipelineOptions options) {
            return Utils.getDefaultJsonFactory();
        }
    }

    /* Once we move to Java 8, the methods in this inner class will become static/default methods
     * in GenomicsPipelineOptions
     */
    class Methods {

        public static GenomicsPipelineOptions create() {
            return PipelineOptionsFactory.create().as(GenomicsPipelineOptions.class);
        }

        public static GenomicsPipelineOptions fromArgs(String[] args) throws IOException {
            return initialize(PipelineOptionsFactory.fromArgs(args).as(GenomicsPipelineOptions.class));
        }

        public static Credential getCredential(GenomicsPipelineOptions options) {
            return new GoogleCredential.Builder()
                    .setTransport(options.getTransport())
                    .setJsonFactory(options.getJsonFactory())
                    .setClientSecrets(options.getClientSecrets())
                    .build()
                    .setFromTokenResponse(options.getTokenResponse());
        }

        public static GenomicsPipelineOptions getFromContext(DoFn<?, ?>.Context context) {
            return context.getPipelineOptions().as(GenomicsPipelineOptions.class);
        }

        public static GenomicsPipelineOptions initialize(GenomicsPipelineOptions options)
                throws IOException {
            JsonFactory jsonFactory = options.getJsonFactory();
            GoogleClientSecrets clientSecrets = loadClientSecrets(jsonFactory, options);
            AuthorizationCodeInstalledApp installedApp = new AuthorizationCodeInstalledApp(
                    new GoogleAuthorizationCodeFlow
                            .Builder(options.getTransport(), jsonFactory, clientSecrets, options.getScopes())
                            .setDataStoreFactory(options.getDataStoreFactory())
                            .build(),
                    options.getVerificationCodeReceiver());
            Credential credential = installedApp.authorize(options.getCredentialId());
            options.setClientSecrets(clientSecrets);
            options.setTokenResponse(new TokenResponse()
                    .setAccessToken(credential.getAccessToken())
                    .setExpiresInSeconds(credential.getExpiresInSeconds())
                    .setRefreshToken(credential.getRefreshToken()));
            return options;
        }

        private static GoogleClientSecrets
        loadClientSecrets(JsonFactory jsonFactory, GcpOptions options) throws IOException {
            try (Reader reader = new FileReader(
                    Preconditions.checkNotNull(options.getSecretsFile(), "--secretsFile is required"))) {
                return GoogleClientSecrets.load(jsonFactory, reader);
            }
        }

        private Methods() {
        }
    }

    class ScopesFactory implements DefaultValueFactory<List<String>> {
        @Override
        public List<String> create(PipelineOptions options) {
            return ImmutableList.<String>builder()
                    .add(DataflowScopes.USERINFO_EMAIL)
                    .add(GenomicsScopes.GENOMICS)
                    .add(StorageScopes.DEVSTORAGE_READ_WRITE)
                    .build();
        }
    }

    class VerificationCodeReceiverFactory implements DefaultValueFactory<VerificationCodeReceiver> {
        @Override
        public VerificationCodeReceiver create(PipelineOptions options) {
            return new LocalServerReceiver();
        }
    }

    class WorkerCredentialFactory implements DefaultValueFactory<Credential> {
        @Override
        public Credential create(PipelineOptions options) {
            GenomicsPipelineOptions genomicsOptions = options.as(GenomicsPipelineOptions.class);
            return new GoogleCredential.Builder()
                    .setTransport(genomicsOptions.getTransport())
                    .setJsonFactory(genomicsOptions.getJsonFactory())
                    .setClientSecrets(genomicsOptions.getClientSecrets())
                    .build()
                    .setFromTokenResponse(genomicsOptions.getTokenResponse());
        }
    }

    GoogleClientSecrets getClientSecrets();

    void setClientSecrets(GoogleClientSecrets clientSecrets);

    @Default.InstanceFactory(DataStoreFactoryFactory.class)
    @JsonIgnore
    DataStoreFactory getDataStoreFactory();

    void setDataStoreFactory(DataStoreFactory dataStoreFactory);

    @Default.InstanceFactory(JsonFactoryFactory.class)
    @JsonIgnore
    JsonFactory getJsonFactory();

    void setJsonFactory(JsonFactory jsonFactory);

    @Default.InstanceFactory(ScopesFactory.class)
    @JsonIgnore
    List<String> getScopes();

    void setScopes(List<String> scopes);

    TokenResponse getTokenResponse();

    void setTokenResponse(TokenResponse tokenResponse);

    @Default.InstanceFactory(HttpTransportFactory.class)
    @JsonIgnore
    HttpTransport getTransport();

    void setTransport(HttpTransport transport);

    @Default.InstanceFactory(VerificationCodeReceiverFactory.class)
    @JsonIgnore
    VerificationCodeReceiver getVerificationCodeReceiver();

    void setVerificationCodeReceiver(VerificationCodeReceiver verificationCodeReceiver);

    @Default.InstanceFactory(WorkerCredentialFactory.class)
    @JsonIgnore
    Credential getWorkerCredential();

    void setWorkerCredential(Credential credential);
}

