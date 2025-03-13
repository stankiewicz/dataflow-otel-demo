package otel.transforms;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.okhttp.v3_0.OkHttpTelemetry;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.beam.sdk.transforms.DoFn;
import otel.utils.OtelProvider;

import java.io.IOException;


public class ApiCallDoFn extends DoFn<String, String> {

    private transient Tracer tracer;
    private transient Call.Factory factory;

    @Setup
    public void setup() throws IOException {
        this.tracer = OtelProvider.get().getTracer("ApiCallDoFn");
        ;
        factory = OkHttpTelemetry.builder(OtelProvider.get()).build().newCallFactory(new OkHttpClient());
    }

    @ProcessElement
    public void processElement(ProcessContext c, @Element String inputString, OutputReceiver<String> output) {

        Span span = tracer.spanBuilder("calling_api").startSpan();
        try (Scope scope = span.makeCurrent()) {
            // Add some Event to the span
            span.addEvent("Before api");
            Request request = new Request.Builder()
                    .url("https://google.com")
                    .build();

            try (Response response = factory.newCall(request).execute()) {
                String s = response.body().string();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            // Add some Event to the span
            span.addEvent("After api");
        } finally {
            span.end();
        }
        String modifiedString = inputString.toUpperCase();
        output.output(modifiedString);
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) {
        Span span = tracer.spanBuilder("finish_bundle").startSpan();
        try (Scope scope = span.makeCurrent()) {

        } finally {
            span.end();
        }
    }
}
