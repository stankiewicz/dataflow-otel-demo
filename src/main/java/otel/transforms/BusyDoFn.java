package otel.transforms;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.apache.beam.sdk.transforms.DoFn;
import otel.utils.OtelProvider;


public class BusyDoFn extends DoFn<String, String> {

  private transient Tracer tracer;

  @Setup
  public void setup() {
    this.tracer = OtelProvider.get().getTracer("BusyDoFn");;
  }

  @ProcessElement
  public void processElement(ProcessContext c, @Element String inputString, OutputReceiver<String> output) {

    Span span = tracer.spanBuilder("processElement").startSpan();
    try (Scope scope = span.makeCurrent()) {
      // Add some Event to the span
      span.addEvent("Event 0");
      // execute my use case - here we simulate a wait
      Thread.sleep(100);
      // Add some Event to the span
      span.addEvent("Event 1");
    } catch (InterruptedException e) {
        throw new RuntimeException(e);
    } finally {
      span.end();
    }

    String modifiedString = inputString.toUpperCase();
    output.output( modifiedString);
  }
}
