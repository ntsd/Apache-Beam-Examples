
from __future__ import absolute_import

import logging

import apache_beam as beam
from apache_beam.utils.pipeline_options import PipelineOptions
from apache_beam.utils.pipeline_options import SetupOptions
import argparse

import csv

class MyCsvFileSource(beam.io.filebasedsource.FileBasedSource):
    def read_records(self, file_name, range_tracker):
        self._file = self.open_file(file_name)

        reader = csv.reader(self._file)

        for rec in reader:
            yield rec

class BusinessRegistrationDoFn(beam.DoFn):

    def __init__(self):
        super(BusinessRegistrationDoFn, self).__init__()

    def process(self, element):
        return element

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='gs://dataflow-test-gg/ThaiBusinessRegistrationData/*',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        # CHANGE 1/5: The Google Cloud Storage path is required
                        # for outputting the results.
                        default='gs://dataflow-test-gg/out/',
                        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        # CHANGE 2/5: (OPTIONAL) Change this to DataflowRunner to
        # run your pipeline on the Google Cloud Dataflow Service.
        '--runner=DataflowRunner',
        # CHANGE 3/5: Your project ID is required in order to run your pipeline on
        # the Google Cloud Dataflow Service.
        '--project=united-night-165213',
        # CHANGE 4/5: Your Google Cloud Storage path is required for staging local
        # files.
        '--staging_location=gs://dataflow-test-gg/AND_STAGING_DIRECTORY',
        # CHANGE 5/5: Your Google Cloud Storage path is required for temporary
        # files.
        '--temp_location=gs://dataflow-test-gg/AND_TEMP_DIRECTORY',
        '--job_name=thai-business-registration',
    ])

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline = beam.Pipeline(options=pipeline_options)

    # Read the text file[pattern] into a PCollection.
    lines = pipeline | 'read file' >> beam.io.ReadFromText(known_args.input)#MyCsvFileSource(known_args.input)#beam.io.ReadFromText(known_args.input)


    output = (lines | 'split' >> (beam.ParDo(BusinessRegistrationDoFn())
                                 .with_output_types(unicode)))


    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | 'write' >> beam.io.WriteToText(known_args.output)

    # Actually run the pipeline (all operations above are deferred).
    pipeline.run().wait_until_finish()



if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()