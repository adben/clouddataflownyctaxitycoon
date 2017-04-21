package nl.adben;


import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import java.util.Date;

import nl.adben.utils.CustomPipelineOptions;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * the pipeline for cost after the count
 */
@SuppressWarnings("serial")
public class SumRides {
    private static final Logger LOG = LoggerFactory.getLogger(SumRides.class);

    public static void main(String[] args) {
        CustomPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomPipelineOptions.class);
        Pipeline p = Pipeline.create(options);

        p.apply(PubsubIO.Read.named("Read from PubSub")
                .topic(String.format("projects/%s/topics/%s", options.getSourceProject(), options.getSourceTopic()))
                .timestampLabel("ts")
                .withCoder(TableRowJsonCoder.of()))
                .apply("Window (60 seconds)",
                        Window.into(
                                SlidingWindows.of(Duration.standardSeconds(60)).every(Duration.standardSeconds(3))))
                .apply("Extract meter increment",
                        MapElements.via((TableRow x) -> Double.parseDouble(x.get("meter_increment").toString()))
                                .withOutputType(TypeDescriptor.of(Double.class)))
                .apply("Summarizes the window", Sum.doublesGlobally().withoutDefaults())
                .apply("Formating the rides",
                        MapElements.via(
                                (Double x) -> {
                                    TableRow r = new TableRow();
                                    r.set("dollar_run_rate_per_minute", x);
                                    LOG.info("Outputting $ value {} at {} ", x, new Date().getTime());
                                    return r;
                                })
                                .withOutputType(TypeDescriptor.of(TableRow.class)))
                .apply(PubsubIO.Write.named("Write To Pubsub")
                        .topic(String.format("projects/%s/topics/%s", options.getSinkProject(), options.getSinkTopic()))
                        .withCoder(TableRowJsonCoder.of()));
        p.run();
    }
}