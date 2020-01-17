################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import logging
import os
import shutil
import sys
import tempfile

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment, TableConfig
from pyflink.table.descriptors import FileSystem, OldCsv, Schema
from pyflink.table.types import DataTypes


def word_count():
    f1 = open("/home/mnm/flink-1.9.1/1", "r")
    f2 = open("/home/mnm/flink-1.9.1/2", "r")
    f3 = open("/home/mnm/flink-1.9.1/3", "r")
    f4 = open("/home/mnm/flink-1.9.1/4", "r")
    f5 = open("/home/mnm/flink-1.9.1/5", "r")
    content=f1.read()+f2.read()+f3.read()+f4.read()+f5.read()

    t_config = TableConfig()
    env = ExecutionEnvironment.get_execution_environment()
    t_env = BatchTableEnvironment.create(env, t_config)

    # register Results table in table environment
    tmp_dir = tempfile.gettempdir()
    result_path = tmp_dir + '/result'
    if os.path.exists(result_path):
        try:
            if os.path.isfile(result_path):
                os.remove(result_path)
            else:
                shutil.rmtree(result_path)
        except OSError as e:
            logging.error("Error removing directory: %s - %s.", e.filename, e.strerror)

    logging.info("Results directory: %s", result_path)

    t_env.connect(FileSystem().path(result_path)) \
        .with_format(OldCsv()
                     .field_delimiter(',')
                     .field("word", DataTypes.STRING())
                     .field("count", DataTypes.BIGINT())) \
        .with_schema(Schema()
                     .field("word", DataTypes.STRING())
                     .field("count", DataTypes.BIGINT())) \
        .register_table_sink("Results")

    elements = [(word, 1) for word in content.split(" ")]
    t_env.from_elements(elements, ["word", "count"]) \
         .group_by("word") \
         .select("word, count(1) as count") \
         .insert_into("Results")

    t_env.execute("Python batch word count")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    word_count()
