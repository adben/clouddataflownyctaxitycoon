package nl.adben;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import nl.adben.utils.CustomPipelineOptions;
import nl.adben.utils.LatLon;
import org.joda.time.Duration;


@SuppressWarnings("serial")
public class CountRides {

    private static class Mark extends SimpleFunction<TableRow, KV<LatLon, TableRow>> {

        @Override
        public KV<LatLon, TableRow> apply(TableRow t) {
            float lat = Float.parseFloat(t.get("latitude").toString());
            float lon = Float.parseFloat(t.get("longitude").toString());
            final float PRECISION = 0.005f;
            float roundedLat = (float) Math.floor(lat / PRECISION) * PRECISION + PRECISION / 2;
            float roundedLon = (float) Math.floor(lon / PRECISION) * PRECISION + PRECISION / 2;
            LatLon key = new LatLon(roundedLat, roundedLon);
            return KV.of(key, t);
        }
    }

    private static class TransformRides extends SimpleFunction<KV<LatLon, Long>, TableRow> {

        @Override
        public TableRow apply(KV<LatLon, Long> ridegrp) {
            TableRow result = new TableRow();
            result.set("latitude", ridegrp.getKey().lat);
            result.set("longitude", ridegrp.getKey().lon);
            result.set("ntaxis", ridegrp.getValue());
            return result;
        }
    }

    public static void main(String[] args) {
        CustomPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomPipelineOptions.class);
        Pipeline p = Pipeline.create(options);

        p.apply(PubsubIO.Read.named("Read from PubSub")
                .topic(String.format("projects/%s/topics/%s", options.getSourceProject(), options.getSourceTopic()))
                .timestampLabel("ts")
                .withCoder(TableRowJsonCoder.of()))
                .apply("Window (1 second)", Window.into(FixedWindows.of(Duration.standardSeconds(1))))
                .apply("Marking", MapElements.via(new Mark()))
                .apply("Counting related", Count.perKey())
                .apply("Formatting", MapElements.via(new TransformRides()))
                .apply(PubsubIO.Write.named("Write ToP ubsub")
                        .topic(String.format("projects/%s/topics/%s", options.getSinkProject(), options.getSinkTopic()))
                        .withCoder(TableRowJsonCoder.of()));
        p.run();
    }
}