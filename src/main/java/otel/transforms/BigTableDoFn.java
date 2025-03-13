package otel.transforms;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.apache.beam.sdk.transforms.DoFn;
import otel.utils.OtelProvider;

import java.io.IOException;


public class BigTableDoFn extends DoFn<String, String> {

  private transient Tracer tracer;
  private transient BigtableDataClient dataClient;

  @Setup
  public void setup() throws IOException {
    this.tracer = OtelProvider.get().getTracer("BigTableDoFn");;
    BigtableDataSettings settings =
            BigtableDataSettings.newBuilder().setProjectId("radoslaws-playground-pso").setInstanceId("barbar").build();
    BigtableDataSettings.enableBuiltinMetrics();
    BigtableDataSettings.enableGfeOpenCensusStats();
    BigtableDataSettings.enableOpenCensusStats();

    dataClient = BigtableDataClient.create(settings);
  }

  @ProcessElement
  public void processElement(ProcessContext c, @Element String inputString, OutputReceiver<String> output) {

    Span span = tracer.spanBuilder("calling_bigtable").startSpan();
    try (Scope scope = span.makeCurrent()) {
      span.addEvent("Before CBT");
      String[] names = {"World", "Bigtable", "Java"};
      for (int i = 0; i < names.length; i++) {
        String greeting = "Hello " + names[i] + "!";
        RowMutation rowMutation =
                RowMutation.create("entries", ""+i)
                        .setCell("f", "v1", names[i])
                        .setCell("f", "v2", greeting);
        dataClient.mutateRow(rowMutation);
      }
      // Add some Event to the span
      span.addEvent("After CBT");
    } finally {
      span.end();
    }
    output.output( inputString);
  }
}
