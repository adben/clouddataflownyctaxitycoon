package nl.adben;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import nl.adben.utils.CustomPipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class FilterRides {
    private static final Logger LOG = LoggerFactory.getLogger(FilterRides.class);

    private static class FilterSouthManhattan extends DoFn<TableRow, TableRow> {
        FilterSouthManhattan() {
        }

        //South Manhattan
        @Override
        public void processElement(ProcessContext c) {
            TableRow ride = c.element();
            float lat = Float.parseFloat(ride.get("latitude").toString());
            float lon = Float.parseFloat(ride.get("longitude").toString());
            if (lon > -74.747 && lon < -73.969) {
                if (lat > 40.699 && lat < 40.720) {
                    c.output(ride);
                }
            }
        }
    }

    public static void main(String[] args) {
        CustomPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomPipelineOptions.class);
        Pipeline p = Pipeline.create(options);

        p.apply(PubsubIO.Read.named("read from PubSub")
                .topic(String.format("projects/%s/topics/%s", options.getSourceProject(), options.getSourceTopic()))
                .timestampLabel("ts")
                .withCoder(TableRowJsonCoder.of()))
                .apply("filter south Manhattan", ParDo.of(new FilterSouthManhattan()))
                .apply(PubsubIO.Write.named("write to PubSub")
                        .topic(String.format("projects/%s/topics/%s", options.getSinkProject(), options.getSinkTopic()))
                        .withCoder(TableRowJsonCoder.of()));
        p.run();
    }
}