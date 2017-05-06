/**
 * Created by Jiravat on 5/6/2017.
 */
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
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

public class ThaiBusinessRegistrationBeam {




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
        String gcp = "united-night-165213"; //set google cloud project ID
        String inputFilePath = "gs://dataflow-test-gg/ThaiBusinessRegistrationData/*";
        String outputFilePath = "gs://dataflow-test-gg/ThaiBusinessRegistrationData/out";
        String tempFilePath = "gs://dataflow-test-gg/tmp";
        args = new String[]{"--runner=DataflowRunner",
                "--project="+gcp,
                "--gcpTempLocation="+tempFilePath,
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
                .apply("Write CSV", TextIO.Write.to(options.getOutput()).withSuffix(".csv"));

        p.run().waitUntilFinish();
    }

}
