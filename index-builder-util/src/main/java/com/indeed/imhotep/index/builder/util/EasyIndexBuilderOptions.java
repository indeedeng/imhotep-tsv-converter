/*
 * Copyright (C) 2014 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.imhotep.index.builder.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;

/**
 * @author mmorrison
 */
public class EasyIndexBuilderOptions extends SmartArgs {
    public String baseDir;
    public long start;
    public long end;
    public String extra;
    public boolean overwrite;
    public boolean skip;

    @Override
    protected void setup() {
        description = "builds index shard for specified time range\n" +
            "start and end must be in yyyy-MM-dd HH:mm:ss format, but mins and secs will be truncated.\n" +
            "DO quote start and end";

        examples = "-o /var/ramses/tmp/indexname -r odin:9091 --start \"2009-06-16 09:00:00\" --end \"2009-06-16 12:00:00\"";

        options.addOption(
                OptionBuilder
                        .isRequired()
                        .withArgName("YYYY-MM-DD HH:MM:SS")
                        .withDescription("start time for processing, can be milliseconds")
                        .hasArg()
                        .withLongOpt("start")
                        .create()
        );
        options.addOption(
                OptionBuilder
                        .isRequired()
                        .withArgName("YYYY-MM-DD HH:MM:SS")
                        .withDescription("end time for processing, can be milliseconds")
                        .hasArg()
                        .withLongOpt("end")
                        .create()
        );
        options.addOption(
                OptionBuilder
                        .isRequired()
                        .withArgName("dir")
                        .withDescription("output directory")
                        .hasArg()
                        .withLongOpt("output")
                        .create("o")
        );
        options.addOption(
                OptionBuilder
                        .withArgName("args")
                        .withDescription("additional arguments")
                        .hasArg()
                        .withLongOpt("extra")
                        .create("e")
        );

        options.addOption(OptionBuilder
            .withDescription("clear this segment if it exists")
            .withLongOpt("overwrite")
            .create()
        );
        options.addOption(OptionBuilder
            .withDescription("exit without error if logs exist")
            .withLongOpt("skip")
            .create()
        );
    }
    @Override
    protected void extract(CommandLine cl) throws Exception {
        start = getOptionTime("start");
        end = getOptionTime("end");

        baseDir = cl.getOptionValue("o");

        extra = cl.getOptionValue("e", "");
        overwrite = cl.hasOption("overwrite");
        skip = cl.hasOption("skip");
    }
}
