/**
 * Created by Jiravat on 5/6/2017.
 */
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StringDelegateCoder;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class ThaiBusinessRegistrationBeamDirect{

    static class ExtractDateFn extends DoFn<String, String> {
        private final Aggregator<Long, Long> emptyLines =
                createAggregator("emptyLines", Sum.ofLongs());

        @ProcessElement
        public void processElement(ProcessContext c) {
            if (c.element().trim().isEmpty()) {
                emptyLines.addValue(1L);
            }

            String date = c.element().trim().split("\\s*,\\s*")[3];

            // Output each word encountered into the output PCollection.
            if (!date.isEmpty()) {
                c.output(date);
            }
        }
    }

    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + "," + input.getValue();
        }
    }

    public static class CountBusiness extends PTransform<PCollection<String>,
            PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

            // Convert lines of text into individual words.
            PCollection<String> date = lines.apply(
                    ParDo.of(new ExtractDateFn()));

            // Count the number of times each word occurs.
            PCollection<KV<String, Long>> businessCounts =
                    date.apply(Count.<String>perElement());

            return businessCounts;
        }
    }

    public interface ThaiBusinessRegistrationOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Default.String("gs://dataflow-test-gg/ThaiBusinessRegistrationData/*")
        String getInputFile();
        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Required
        String getOutput();
        void setOutput(String value);
    }

    public static void main(String[] args) {
        String inputFilePath = "businessRegistrationsData/99_201601.csv";
        String outputFilePath = "businessRegistrationsData/outs";
        args = new String[]{
                "--inputFile="+inputFilePath,
                "--output="+outputFilePath};
        ThaiBusinessRegistrationOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(ThaiBusinessRegistrationOptions.class);
//    options.setInputFile("gs://dataflow-test-gg/ThaiBusinessRegistrationData/*");
        options.setJobName("ThaiBusinessRegistrationBeam");
//    options.setOutput("gs://dataflow-test-gg/ThaiBusinessRegistrationData/out");
        Pipeline p = Pipeline.create(options);

        p.apply("ReadLines", TextIO.Read.from(options.getInputFile()))
                .setCoder(StringUtf8Coder.of())
                .apply(new CountBusiness())
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply("Write CSV", TextIO.Write.to(options.getOutput()).withSuffix(".csv"));

        p.run().waitUntilFinish();
    }



}
