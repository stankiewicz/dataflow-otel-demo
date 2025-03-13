package otel.utils;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import com.google.common.collect.ImmutableList;

public class SetupTraceDoFn extends DoFn<PubsubMessage, String> {
  private transient Tracer tracer;
  @Setup
  public void setup() {
    this.tracer = OtelProvider.get().getTracer("SetupTraceDoFn");;
  }

  Context extractSpanContext(PubsubMessage message) {
    TextMapGetter<PubsubMessage> extractMessageAttributes =
            new TextMapGetter<PubsubMessage>() {
              @Override
              public String get(PubsubMessage carrier, String key) {
                  assert carrier != null;
                  return carrier.getAttribute("googclient_" + key);
              }

              public Iterable<String> keys(PubsubMessage carrier) {
                return carrier.getAttributeMap()==null? ImmutableList.of():carrier.getAttributeMap().keySet();
              }
            };
      return W3CTraceContextPropagator.getInstance()
              .extract(Context.current(), message, extractMessageAttributes);
  }

  @ProcessElement
  public void processElement(ProcessContext c, @Element PubsubMessage message, OutputReceiver<String> output) {
    Context context = extractSpanContext(message);
    Span psSub = tracer.spanBuilder("PubSub_sub").setParent(context).startSpan();
    try(Scope s = context.makeCurrent()){
      output.output(new String(message.getPayload()));
      psSub.setStatus(StatusCode.OK);
    }finally {

      psSub.end();
    }
  }
}
