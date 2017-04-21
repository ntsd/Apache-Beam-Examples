package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.MapElements;

/**
 * Created by Jiravat on 4/21/2017.
 */
public class LiquorPriceMatching {

    public interface LiquorPriceOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Default.String("gs://apache-beam-samples/shakespeare/*")
        String getInputFile();
        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Validation.Required
        String getOutput();
        void setOutput(String value);
    }

    public static void main(String[] args) {
        String gcp = "united-night-165213"; //set google cloud project ID
        String gcs = "dataflow-test-gg/LiquorPrice"; //set google cloud bucket path
        String inputFilePath = "gs://dataflow-test-gg/LiquorPrice/BC_Liquor_Store_Product_Price_List.csv";
        args = new String[]{"--runner=DataflowRunner",
                "--project="+gcp,
                "--gcpTempLocation=gs://"+gcs+"/tmp",
                "--inputFile="+inputFilePath,
                "--output=gs://"+gcs+"/outs"};
        LiquorPriceOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(LiquorPriceOptions.class);
//    options.setInputFile("gs://apache-beam-samples/shakespeare/*");
//    options.setOutput("gs://dataflow-test-gg/counts");
        Pipeline p = Pipeline.create(options);

        p.apply("ReadLines", TextIO.Read.from(options.getInputFile()))
                .apply("WriteMatching", TextIO.Write.to(options.getOutput()));

        p.run().waitUntilFinish();
    }
}
