package otel.utils;

import com.google.cloud.opentelemetry.trace.TraceExporter;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;

public class OtelProvider {
    private static OpenTelemetry instance = null;

    public static synchronized OpenTelemetry get() {
        if (instance != null) return instance;
        SpanExporter traceExporter = TraceExporter.createWithDefaultConfiguration();
        Resource resource =
                Resource.getDefault()
                        .toBuilder()
                        .put("service.name", "my_dataflow")
                        .build();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(traceExporter).build())
                .addResource(resource)
                .build();
        instance = OpenTelemetrySdk.builder()
                .setTracerProvider(
                        tracerProvider)
                .buildAndRegisterGlobal();
        return instance;
    }

}
