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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author mmorrison
 *
 * Another command line argument parser.
 *
 * 1. Extend this class.
 * 2. In setup(), add parsable fields to `options`. Optionally
 * set program description and command line examples.
 * 3. In extract(), parse your fields however you wish from the result
 * object given. If an argument's value is invalid or not parsable, simply
 * throw an org.apache.commons.cli.ParseException. Any thrown exceptions
 * will result in usage being printed, followed by exit.
 * 4. Call parse(args) in your main program.
 *
 * - For added flexibility, additional arguments can be added with addExtraOptions(),
 * and those extra options can be parsed after parse() with getResults().
 *
 * - If PROMPT_FOR_INPUT or IDEA_HOME environment variable is set, and no arguments are
 * passed to the program, the class will
 * automatically prompt for input parameters (nice for testing in IDEs).
 */
public abstract class SmartArgs {
    protected Options options = new Options();
    protected String description = "";
    protected String examples = "";
    private CommandLine results = null;
    private List<SmartArgs> extensions = new ArrayList<SmartArgs>();

    protected abstract void setup();

    protected abstract void extract(CommandLine results) throws Exception;

    protected void usage() {
        StringWriter helpWriter = new StringWriter();
        PrintWriter helpPrintWriter = new PrintWriter(helpWriter);
        new HelpFormatter().printHelp(helpPrintWriter, 80, " ", "", options, 1, 2, "");
        helpPrintWriter.close();
        String params = helpWriter.toString();
        params = params.replaceFirst("usage", "Parameters");

        if(!description.isEmpty()) {
            System.err.println("Description:\n"+description+"\n");
        }
        System.err.println(params);
        if(!examples.isEmpty()) {
            System.err.println("Examples:\n"+examples+"\n");
        }
    }

    private boolean shouldPromptForInput() {
        if(System.getenv("PROMPT_FOR_INPUT") != null) return true;
        if(System.getenv("IDEA_HOME") != null) return true;
        return false;
    }
    private String[] promptForInput() {
        System.err.println("PROMPT_FOR_INPUT environment variable set.");
        System.err.println("Please enter the parameters for this program:");
        try {
            String line = new BufferedReader(new InputStreamReader(System.in)).readLine();
            return parseArgParams(line);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new String[0];
    }

    public void addExtension(SmartArgs other) {
        extensions.add(other);
    }
    public long getOptionTime(String option) throws ParseException {
        String value = results.getOptionValue(option);

        try {
            return Long.parseLong(value);
        } catch(Exception e) {
        }

        try {
            return Timestamp.valueOf(value).getTime();
        } catch(Exception e) {
        }

        throw new ParseException("Invalid time for option: "+option);
    }

    /**
     * Parses an ISO-8601 formatted date time string
     * Format: YYYY-MM-DDThh:mm:ss, ex. 2013-03-04T18:00:00 -> 6 p.m. on Mar. 4, 2013
     * @param optionName The name of the option to parse
     * @return Milliseconds of the parsed {@link DateTime}
     * @throws ParseException
     */
    public long getOptionDateTime(@Nonnull final String optionName) throws ParseException {
        final String value = results.getOptionValue(optionName);
        return parseOptionDateTime(value, optionName);
    }

    /**
     * Parses an ISO-8601 formatted date time string
     * Format: YYYY-MM-DDThh:mm:ss, ex. 2013-03-04T18:00:00 -> 6 p.m. on Mar. 4, 2013
     * @param value Option value
     * @param optionName The option name used for helpful exception messages.
     * @return Milliseconds of the parsed {@link DateTime}
     * @throws ParseException
     */
    @VisibleForTesting
    protected static long parseOptionDateTime(@Nonnull String value,
                                              @Nonnull final String optionName) throws ParseException {
        try {
            return new DateTime(value, DateTimeZone.forID("US/Central")).getMillis();
        } catch (final IllegalArgumentException e) {
            throw new ParseException("Could not parse " + optionName + " into a valid DateTime. Value: " + value);
        }
    }

    /**
     * Parses an integer value, also gives a slightly more helpful error message
     * @param optionName The name of the option to parse
     * @return The parsed integer value
     * @throws ParseException
     */
    public int getIntValue(@Nonnull final String optionName) throws ParseException {
        final String value = results.getOptionValue(optionName);
        try {
            return Integer.parseInt(value);
        } catch (final IllegalArgumentException e) {
            throw new ParseException("Could not parse \"" + value + "\" into a valid Integer for " + optionName);
        }
    }

    public void parse(String[] args) {
        setup();
        // merge in the options from extensions
        for(SmartArgs other : extensions) {
            other.setup();
            for(Option option : (Collection<Option>)other.options.getOptions()) {
                options.addOption(option);
            }
        }

        boolean launchedWithArgs = (args.length != 0);

        if(!launchedWithArgs && shouldPromptForInput()) {
            usage();
            args = promptForInput();
        }

        while(true) {
            try {
                results = new PosixParser().parse(options, args);
                extract(results);
                for(SmartArgs other : extensions) {
                    other.extract(results);
                }
            } catch(Exception e) {
                // Yes, yes, yes... catching all Exceptions can be bad. However, in our case,
                // we want ANY parsing errors, integer casts in extract, or anything else to just
                // dump usage and exit.
                results = null;

                System.err.println("Parameter Error - "+e.getMessage());

                if(!launchedWithArgs && shouldPromptForInput()) {
                    System.err.println();
                    args = promptForInput();
                    continue;
                } else {
                    usage();
                    System.exit(-1);
                }
            }

            return;
        }
    }

    /**
     * parse a String into an argument list, in the same way java parses
     * arguments passed to main()
     */
    public static String[] parseArgParams(String line) {
        StreamTokenizer tok = new StreamTokenizer(new StringReader(line));
        tok.resetSyntax();
        tok.wordChars('\u0000','\uFFFF');
        tok.whitespaceChars(' ', ' ');
        tok.quoteChar('\"');
        ArrayList<String> output = new ArrayList<String>();
        try {
            while (tok.nextToken() != StreamTokenizer.TT_EOF) {
                output.add(tok.sval);
            }
        } catch(IOException e) {
        }
        return output.toArray(new String[output.size()]);
    }
}
