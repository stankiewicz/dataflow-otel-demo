// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

package otel;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import otel.transforms.ApiCallDoFn;
import otel.transforms.BigTableDoFn;
import otel.transforms.BusyDoFn;
import otel.utils.SetupTraceDoFn;


public class OtelDemo {
	public interface Options extends PipelineOptions {

		@Description("Input sub.")
		@Default.String("projects/radoslaws-playground-pso/subscriptions/mytopic-sub")
		String getSub();
		void setSub(String value);
	}

	public static void buildPipeline(Pipeline pipeline, Options options) {

		PCollection<PubsubMessage> events;
		String sub = options.getSub();
		events = pipeline
				.apply("Read from PS", PubsubIO.readMessagesWithAttributes().fromSubscription(sub));

		PCollection<String> b = events.apply("SetupTrace", ParDo.of(new SetupTraceDoFn())).
				 apply("busyWork", ParDo.of(new BusyDoFn()));
		b.apply("cbt", ParDo.of(new BigTableDoFn()));
		b.apply("api", ParDo.of(new ApiCallDoFn()));

	}

	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
		OtelDemo.buildPipeline(pipeline, options);
		pipeline.run().waitUntilFinish();
	}
}