package org.broadinstitute.hellbender.engine.dataflow;

/**
 * Created by davidada on 5/7/15.
 */

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.util.Key;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.util.Collections;

/**
 * This is just a temporary hack until pipeline option json serialization is plumbed into workers.
 */
public class WorkerCredentialFactory extends GenericJson {

    private static final TypeToken<WorkerCredentialFactory>
            TYPE_TOKEN = new TypeToken<WorkerCredentialFactory>() {
    };

    public static WorkerCredentialFactory of(Pipeline pipeline) {
        CoderRegistry registry = pipeline.getCoderRegistry();
        if (null == registry.getDefaultCoder(TYPE_TOKEN)) {
            registry.registerCoder(
                    WorkerCredentialFactory.class,
                    GenericJsonCoder.of(WorkerCredentialFactory.class));
        }
        GenomicsPipelineOptions options = pipeline.getOptions().as(GenomicsPipelineOptions.class);
        WorkerCredentialFactory factory = new WorkerCredentialFactory();
        factory.setClientSecrets(Preconditions.checkNotNull(options.getClientSecrets(), "Must set ClientSecrets on options"));
        factory.setTokenResponse(options.getTokenResponse());
        return factory;
    }

    public static PCollectionView<WorkerCredentialFactory> create(Pipeline pipeline) {
        CoderRegistry registry = pipeline.getCoderRegistry();
        if (null == registry.getDefaultCoder(TYPE_TOKEN)) {
            registry.registerCoder(
                    WorkerCredentialFactory.class,
                    GenericJsonCoder.of(WorkerCredentialFactory.class));
        }
        GenomicsPipelineOptions options = pipeline.getOptions().as(GenomicsPipelineOptions.class);
        WorkerCredentialFactory factory = new WorkerCredentialFactory();
        factory.setClientSecrets(Preconditions.checkNotNull(options.getClientSecrets(), "Must set ClientSecrets on options"));
        factory.setTokenResponse(options.getTokenResponse());
        return pipeline
                .apply(Create.of(Collections.singletonList(factory)).withName("WorkerCredentialFactory"))
                .apply(View.<WorkerCredentialFactory>asSingleton());
    }

    @Key("clientSecrets")
    private GoogleClientSecrets clientSecrets;
    @Key("tokenResponse")
    private TokenResponse tokenResponse;

    public GoogleClientSecrets getClientSecrets() {
        return clientSecrets;
    }

    public Credential getCredential(HttpTransport transport, JsonFactory jsonFactory) {
        return new GoogleCredential.Builder()
                .setTransport(Preconditions.checkNotNull(transport, "transport"))
                .setJsonFactory(Preconditions.checkNotNull(jsonFactory, "jsonFactory"))
                .setClientSecrets(Preconditions.checkNotNull(getClientSecrets(), "client secrets"))
                .build()
                .setFromTokenResponse(getTokenResponse());
    }

    public TokenResponse getTokenResponse() {
        return tokenResponse;
    }

    public void setClientSecrets(GoogleClientSecrets clientSecrets) {
        this.clientSecrets = clientSecrets;
    }

    public void setTokenResponse(TokenResponse tokenResponse) {
        this.tokenResponse = tokenResponse;
    }
}