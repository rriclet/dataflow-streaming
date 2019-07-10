package org.apache.beam.gobblin;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;

public class PubSubToGcs {

    public interface PubSubToGcsOptions extends PipelineOptions {
        @Description("Environment [dev | prod]")
        @Default.String("dev")
        @Validation.Required
        String getEnv();
        void setEnv(String env);
    }

    private static final String subs[] = {
        "sub1",
        "sub2",
        "sub3"
    };

    private static PubSubToGcsOptions getOptions(String[] args) {
        // Add Environment option
        PipelineOptionsFactory.register(PubSubToGcsOptions.class);
        PubSubToGcsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToGcsOptions.class);

        final String ENV = options.getEnv();
        final String PROJECT_PREFIX = "myproject-" + ENV;
        final String BUCKET = "gs://" + PROJECT_PREFIX + "-gcs-input";

        // Set all options
        options.setJobName("pubsub-to-gcs" + ENV);
        options.setRunner(DataflowRunner.class);

        DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
        dataflowOptions.setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);
        dataflowOptions.setMaxNumWorkers(2);
        dataflowOptions.setProject(PROJECT_PREFIX + "-data");
        dataflowOptions.setRegion("europe-west1");
        dataflowOptions.setServiceAccount(PROJECT_PREFIX + "-sac-dataflow@" + PROJECT_PREFIX + "-data.iam.gserviceaccount.com");
        dataflowOptions.setTempLocation(BUCKET + "/temp");
        dataflowOptions.setTemplateLocation(BUCKET + "/templates/pubsub-to-gcs");
        dataflowOptions.setWorkerMachineType("n1-standard-2");

        DataflowPipelineWorkerPoolOptions workerOptions = options.as(DataflowPipelineWorkerPoolOptions.class);
        workerOptions.setSubnetwork("https://www.googleapis.com/....");
        workerOptions.setZone("europe-west1-b");

        StreamingOptions streamingOptions = options.as(StreamingOptions.class);
        streamingOptions.setStreaming(true);

        return options;
    }

    private static class DatePartitionedName implements FileIO.Write.FileNaming {
        private static final DateTimeFormatter DATE_FORMAT = DateTimeFormat.forPattern("yyyy/MM/dd");
        private static final DateTimeFormatter TIME_FORMAT = DateTimeFormat.forPattern("HH:mm:ss");

        private String filePrefix;

        DatePartitionedName(String prefix) {
            filePrefix = prefix;
        }

        @Override
        public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
            IntervalWindow intervalWindow = (IntervalWindow) window;

            return String.format(
                "%s/%s/%s-%s-%s-%s-of-%s%s",
                filePrefix,
                DATE_FORMAT.print(intervalWindow.start()),
                filePrefix,
                TIME_FORMAT.print(intervalWindow.start()),
                TIME_FORMAT.print(intervalWindow.end()),
                shardIndex,
                numShards,
                compression.getSuggestedSuffix());
        }
    }

    public static void main(String[] args) {
        PubSubToGcsOptions options = getOptions(args);

        final String PROJECT = "myproject-" + options.getEnv();
        final String SUB_PREFIX = "projects/" + PROJECT + "-data/subscriptions/" + PROJECT + "-sub-";
        final String BUCKET = "gs://" + PROJECT + "-gcs-input/topics/";

        Pipeline p = Pipeline.create(options);

        // Read PubSub subscriptions and map messages with subscription name
        ArrayList<PCollection<KV<String, String>>> pCollections = new ArrayList<>();
        for (String s : subs) {
            PCollection<KV<String, String>> messages = p
                .apply("Read " + s, PubsubIO.<String>readStrings().fromSubscription(SUB_PREFIX + s))
                .apply("Map " + s, MapElements.via(new SimpleFunction<String, KV<String, String>>() {
                    @Override
                    public KV<String, String> apply(String input) {
                        return KV.of(s, input);
                    }
                }));
            pCollections.add(messages);
        }

        PCollectionList.of(pCollections)
            .apply("Flatten", Flatten.pCollections())
            .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            .apply("Window", Window.<KV<String, String>>into(FixedWindows.of(Duration.standardMinutes(10)))
                .discardingFiredPanes()
            )
            .apply("Write", FileIO.<String, KV<String, String>>writeDynamic()
                .withDestinationCoder(StringUtf8Coder.of())
                .by(KV::getKey)
                .via(Contextful.fn(KV::getValue), TextIO.sink())
                .withNumShards(1)
                .to(BUCKET)
                .withNaming(DatePartitionedName::new)
                .withCompression(Compression.GZIP)
            );

        p.run();
    }
}
