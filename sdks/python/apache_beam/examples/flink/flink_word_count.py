#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A streaming workflow that uses a synthetic streaming source.

This can only be used with the Flink portable runner.
"""

# pytype: skip-file

import argparse
import logging
import sys
import re

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions



def run(argv=None):
  """Build and run the pipeline."""
  args = [
    "--runner=PortableRunner", "--job_endpoint=localhost:8099", "--environment_type=LOOPBACK", "--max_bundle_size=1",
  ]
  if argv:
    args.extend(argv)

  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(args)
  pipeline_options = PipelineOptions(pipeline_args)

  with beam.Pipeline(options=pipeline_options) as p:

    # Read the text file[pattern] into a PCollection.
    lines = p | beam.Create([
        "Hello, world!",
        "Hello, beam!",
        "Hello, flink!",
        "Hello, python!",
        "Hello, java!",
    ])

    # Count the occurrences of each word.
    counts = (
        lines
        | 'Split' >> (
            beam.FlatMap(
                lambda x: re.findall(r'[A-Za-z\']+', x)).with_output_types(str))
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum))

    # Format the counts into a PCollection of strings.
    def format_result(word_count):
      (word, count) = word_count
      return '%s: %s' % (word, count)

    output = counts | 'Format' >> beam.Map(format_result)

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | beam.Map(print)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run(sys.argv[1:])
