/*
 * Copyright (C) 2018 Indeed Inc.
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
 package com.indeed.imhotep.builder.tsv;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.indeed.util.core.threads.NamedThreadFactory;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.shell.PosixFileOperations;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.archive.SquallArchiveWriter;
import com.indeed.imhotep.archive.compression.SquallArchiveCompressor;
import com.indeed.imhotep.index.builder.util.EasyIndexBuilderOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author vladimir
 */

public class TsvConverter {
    static final Logger log = Logger.getLogger(TsvConverter.class);
    private static final List<String> ALLOWED_FILE_EXT = Lists.newArrayList(".tsv", ".tsv.gz", ".csv", ".csv.gz");
    private static final String ALLOWED_INDEX_NAMES = "[a-z][a-zA-Z0-9]*";
    private static final DateTimeFormatter yyyymmdd = DateTimeFormat.forPattern("yyyyMMdd");
    private static final DateTimeFormatter yyyymmddhh = DateTimeFormat.forPattern("yyyyMMdd.HH");
    private static final DateTimeFormatter yyyymmdddothh = DateTimeFormat.forPattern("yyyyMMdd.HH");
    private static final DateTimeFormatter yyyymmddhhmmss = DateTimeFormat.forPattern("yyyyMMddHHmmss");
    private static final String DEFAULT_KEYTAB_PATH = "/etc/krb5.keytab";
    private static int DEFAULT_PARALLEL_BUILDS = 4;

    private static boolean DEBUG_BUILD_ONE = false;

    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
        TimeZone.setDefault(TimeZone.getTimeZone("GMT-6"));
    }
    
    Path toIndexPath;
    Path processedSuccessPath;
    Path processedFailedPath;
    Path finalShardPath;
    String toIndexDir = null;
    String processedSuccessDir = null;
    String processedFailedDir = null;
    String finalShardDir = null;
    File localBuildDir;
    FileSystem inputFS;
    FileSystem finalFS;
    boolean qaMode;
    Configuration hdfsConf;
    ExecutorService executor;

    public TsvConverter() {
    }

    public void init(String indexDir,
                     String successDir,
                     String failedDir,
                     String shardDir,
                     String buildDir,
                     String principal,
                     String keytab,
                     boolean qa,
                     String parallelBuildsString) {
        if (Strings.isNullOrEmpty(indexDir) 
                || Strings.isNullOrEmpty(successDir)
                || Strings.isNullOrEmpty(shardDir)
                || Strings.isNullOrEmpty(failedDir)) {
            log.error("directories have to be set in the properties config file");
            System.exit(-1);
        }
        
        toIndexDir = indexDir;
        processedSuccessDir = successDir;
        processedFailedDir = failedDir;
        finalShardDir = shardDir;
        qaMode = qa;

        final int parallelBuilds;
        try {
            parallelBuilds = Integer.parseInt(parallelBuildsString);
        } catch (Exception e) {
            log.error("Argument to 'parallel-builds' must be a positive integer. Given: " + parallelBuildsString);
            System.exit(-1);
            return;
        }
        executor = new ThreadPoolExecutor(
                parallelBuilds, 10, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(10000),
                new NamedThreadFactory("Builder"));

        if (toIndexDir.startsWith("hdfs:") 
                || processedSuccessDir.startsWith("hdfs:")
                || finalShardDir.startsWith("hdfs")) {
            kerberosLogin(principal, keytab);
        }
        hdfsConf = new org.apache.hadoop.conf.Configuration();
        inputFS = getFS(toIndexDir);
        finalFS = getFS(finalShardDir);
        // TODO: should we make the paths qualified?
        toIndexPath = inputFS.makeQualified(new Path(toIndexDir));
        processedSuccessPath = inputFS.makeQualified(new Path(processedSuccessDir));
        processedFailedPath = inputFS.makeQualified(new Path(processedFailedDir));
        finalShardPath = finalFS.makeQualified(new Path(finalShardDir));

        // TODO: delete on completion?
        localBuildDir = (buildDir == null) ? Files.createTempDir() : new File(buildDir);

        for(Path path : Lists.newArrayList(toIndexPath, processedSuccessPath, processedFailedPath, finalShardPath)) {
            checkPathExists(path);
        }
    }

    private void kerberosLogin(String principal, String keytab) {
        try {
            KerberosUtils.loginFromKeytab(principal, keytab);
        } catch (IOException e) {
            log.error("Failed to log in to Kerberos which is necessary to access HDFS", e);
            System.exit(1);
        }
    }

    private void checkPathExists(Path path) {
        boolean exists;
        FileSystem fs;
        Path qualifiedPath = path;
        try {
            fs = getFS(path);
            qualifiedPath = path.makeQualified(fs);
            exists = fs.exists(path);
        } catch (Exception e) {
            exists = false;
        }
        if(!exists) {
            throw new RuntimeException("The provided path doesn't exist " + qualifiedPath.toString() +
                "\nFor hdfs files use 'hdfs:' prefix like hdfs:/tmp/file.tsv" +
                "\nFor local files use 'file://' prefix like file:/tmp/file.tsv");
        }
    }

    private FileSystem getFS(String path) {
        return getFS(new Path(path));
    }

    private FileSystem getFS(Path path) {
        try {
            return path.getFileSystem(hdfsConf);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public void run() {
        final List<FileToIndex> files = findNewFilesToIndex();

        boolean first = true;
        final Map<FileToIndex, Future<Boolean>> futures = Maps.newHashMap();
        final int toProcessCount = files.size();
        final AtomicInteger processedCount = new AtomicInteger(0);
        for(final FileToIndex fileToIndex : files) {
            if(DEBUG_BUILD_ONE && !first) {
                break;
            }
            first = false;

            futures.put(fileToIndex, executor.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    final ShardInfo.DateTimeRange fileTimeRange = getTimeRangeFromFileName(fileToIndex.name);

                    final String indexName = fileToIndex.index;
                    final File localShardParentDir = localBuildDir;

                    final String shardName = getShardName(fileTimeRange);

                    final File localShardDir = new File(localShardParentDir, shardName);
                    final String[] commandToRun = new String[] {
                            "-o", localShardParentDir.getAbsolutePath(),
                            "--start", fileTimeRange.start.toString().replace("T", " ").substring(0, 19),
                            "--end", fileTimeRange.end.toString().replace("T", " ").substring(0, 19),
                            "--overwrite",
                            "--extra", fileToIndex.fsPath.toString().replaceFirst("hdfs://[^/]*", "hdfs:")
                    };

                    final EasyIndexBuilderOptions builderOptions = new EasyIndexBuilderOptions();
                    builderOptions.baseDir = localShardParentDir.getAbsolutePath();
                    builderOptions.start = fileTimeRange.start.getMillis();
                    builderOptions.end = fileTimeRange.end.getMillis();
                    builderOptions.overwrite = true;
                    builderOptions.extra = fileToIndex.fsPath.toString().replaceFirst("hdfs://[^/]*", "hdfs:");

                    log.info(StringUtils.join(commandToRun, " "));
                    final EasyIndexBuilderFromTSV builder = new EasyIndexBuilderFromTSV();

                    builder.setOptions(builderOptions);
                    final int buildExitCode = builder.build();

                    // alternatively run in a separate process instead of in the same thread
//                    final String bashCommand = "\"" + org.apache.commons.lang.StringUtils.join(commandToRun, "\" \"") + "\" 2>&1";
//                    log.info(bashCommand);
//                    final Process runningBuild = Runtime.getRuntime().exec(new String[] {"bash", "-c", bashCommand});
//                    BufferedReader output = new BufferedReader(new InputStreamReader(runningBuild.getInputStream()));
//                    for(String line = output.readLine(); line != null; line = output.readLine()) {
//                        log.info(line); // TODO
//                    }
//                    final int buildExitCode = runningBuild.waitFor();

                    if(buildExitCode != 0) {
                        throw new RuntimeException("Build exited with a non 0 code: " + buildExitCode);
                    }
                    if(!localShardDir.exists() || !localShardDir.isDirectory()) {
                        throw new RuntimeException("Build completed but a shard directory is not found at " + localShardDir);
                    }

                    // add build timestamp (version)
                    final File localShardDirWithTimestamp = new File(localShardParentDir, localShardDir.getName() + "." +
                            yyyymmddhhmmss.print(DateTime.now()));

                    if(!localShardDir.renameTo(localShardDirWithTimestamp)) {
                        throw new RuntimeException("Failed to append timestamp to completed shard dir: " + localShardDir);
                    }

                    final String scheme = finalFS.getUri().getScheme();
                    if(scheme.equals("hdfs") || scheme.equals("s3n") || scheme.equals("s3a")) {
                        final boolean uploadSucceeded =
                                uploadShard(localShardDirWithTimestamp.getParent(),
                                            localShardDirWithTimestamp.getName(),
                                            indexName,
                                            finalShardPath,
                                            finalFS,
                                            qaMode);
                        if(uploadSucceeded) {
                            try {
                                PosixFileOperations.rmrf(localShardDirWithTimestamp);
                            } catch (IOException e) {
                                log.warn("Failed to delete temp dir: " + localShardDirWithTimestamp, e);
                            }
                        } else {
                            log.error("Shard upload failed. Local shard copy left in: " + localShardDirWithTimestamp);
                            throw new RuntimeException("Failed to upload the built shard to " + scheme);
                        }
                    } else {
                        final Path localShardLoc;
                        final Path finalIndexPath;
                        final Path finalShardLoc;
                        
                        localShardLoc = new Path("file:" + localShardDirWithTimestamp.getAbsolutePath());
                        finalIndexPath = new Path(finalShardPath, indexName);
                        finalFS.mkdirs(finalIndexPath);
                        finalShardLoc = new Path(finalIndexPath, localShardDirWithTimestamp.getName());
                        
                        if (!finalFS.rename(localShardLoc, finalShardLoc)) {
                            throw new RuntimeException("Failed to move the completed shard dir "
                                    + localShardDir + " to the final location at " + finalIndexPath);
                        }
                    }

                    moveToProcessed(fileToIndex, processedSuccessPath);

                    log.info("Progress: " + (processedCount.incrementAndGet() * 100 / toProcessCount) + "%");

                    return true;
                }
            }));
        }
        for(FileToIndex fileToIndex : files) {
            final Future<Boolean> future = futures.get(fileToIndex);
            if(future == null) {
                log.warn("Build failed for: " + fileToIndex.fsPath);
                continue;
            }
            boolean success;
            try {
                success = future.get();
            } catch (Exception e) {
                log.warn("Failed to build " + fileToIndex.fsPath, e);
                success = false;
                try {
                    writeExceptionLog(fileToIndex, processedFailedPath, e);
                } catch (Exception e2) {
                    log.error("Failed to write the error log to HDFS", e2);
                }
            }
            if(!success) {
                // mark the file as failed
                moveToProcessed(fileToIndex, processedFailedPath);
            }
        }
    }

    private void writeExceptionLog(FileToIndex fileToIndex, Path processedParentDirPath, Exception e) throws IOException{
        final Path processedIndexDirPath = getIndexDirUnderPath(fileToIndex, processedParentDirPath);
        final FSDataOutputStream logStreamRaw = inputFS.create(new Path(processedIndexDirPath, fileToIndex.name + ".error.log"));
        try {
            final PrintStream logStream = new PrintStream(logStreamRaw);
            e.printStackTrace(logStream);
            Closeables2.closeQuietly(logStream, log);
        } finally {
            Closeables2.closeQuietly(logStreamRaw, log);
        }
    }

    private void moveToProcessed(FileToIndex fileToIndex, Path processedParentDirPath) {
        // move the input file to the 'processed' or 'failed' dir
        Path processedIndexDirPath = null;
        try {
            processedIndexDirPath = getIndexDirUnderPath(fileToIndex, processedParentDirPath);
            final Path processedFilePath = new Path(processedIndexDirPath, fileToIndex.fsPath.getName());
            if(inputFS.exists(processedFilePath)) {
                log.info("Deleting older processed TSV: " + processedFilePath);
                inputFS.delete(processedFilePath, false);
            }
            if(!toIndexPath.toString().equals(processedParentDirPath.toString())) {
                inputFS.rename(fileToIndex.fsPath, processedFilePath);
            }
        } catch (IOException e) {
            log.warn("Failed to move processed file " + fileToIndex.fsPath + " to " + processedIndexDirPath, e);
        }
    }

    private Path getIndexDirUnderPath(FileToIndex fileToIndex, Path processedParentDirPath) throws IOException {
        final Path processedIndexDirPath = new Path(processedParentDirPath, fileToIndex.index);
        if(!inputFS.exists(processedIndexDirPath)) {
            inputFS.mkdirs(processedIndexDirPath);
            // TODO: what perms do we want?
            makeWorldWritable(inputFS, processedIndexDirPath);
        }
        return processedIndexDirPath;
    }

    private ShardInfo.DateTimeRange getTimeRangeFromFileName(String filename) {
        final String nameStandardized = "index" + filename
                .replaceFirst("[^0-9]*$", "")    // remove everything that comes after the time range
                .replaceFirst("^[^0-9]*", "");   // get rid of all non digits at the beginning of the name
        return parseDateTime(nameStandardized);
    }

    public static ShardInfo.DateTimeRange parseDateTime(String shardId) {
        try {
            if (shardId.length() > 16) {
                final DateTime start = yyyymmddhh.parseDateTime(shardId.substring(5, 16));
                final DateTime end = yyyymmddhh.parseDateTime(shardId.substring(17, 28));
                return new ShardInfo.DateTimeRange(start, end);
            } else if (shardId.length() > 13) {
                final DateTime start = yyyymmddhh.parseDateTime(shardId.substring(5, 16));
                final DateTime end = start.plusHours(1);
                return new ShardInfo.DateTimeRange(start, end);
            } else {
                final DateTime start = yyyymmdd.parseDateTime(shardId.substring(5, 13));
                final DateTime end = start.plusDays(1);
                return new ShardInfo.DateTimeRange(start, end);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to get time range from file name: " + shardId, e);
        }
    }

    private String getShardName(ShardInfo.DateTimeRange fileTimeRange) {
        final String shardStart = yyyymmdddothh.print(fileTimeRange.start);
        final String shardEnd = yyyymmdddothh.print(fileTimeRange.end);
        return "index" + shardStart + "-" + shardEnd;
    }

    private List<FileToIndex> findNewFilesToIndex() {
        try {
            final List<FileToIndex> files = Lists.newArrayList();
            for(FileStatus dir : inputFS.listStatus(toIndexPath)) {
                if(!dir.isDir()) {
                    continue;
                }
                final Path indexPath = dir.getPath();
                final String indexName = indexPath.getName();
                if(!indexName.matches(ALLOWED_INDEX_NAMES)) {
                    log.info("Skipped directory " + indexPath + ". Index names should match regex " + ALLOWED_INDEX_NAMES);
                    continue;
                }
                for(FileStatus file : inputFS.listStatus(indexPath)) {
                    if(file.isDir()) {
                        continue;
                    }
                    final Path filePath = file.getPath();
                    String fileName = filePath.getName();

                    boolean extFound = false;
                    for(String allowedExt : ALLOWED_FILE_EXT) {
                        if(!fileName.endsWith(allowedExt)) {
                            continue;
                        }
                        fileName = fileName.substring(0, fileName.length() - allowedExt.length());
                        files.add(new FileToIndex(fileName, indexName, filePath));
                        extFound = true;
                        break;
                    }
                    if(!extFound) {
                        log.info("Not one of supported extensions (" + StringUtils.join(ALLOWED_FILE_EXT, ", ") + ") file: " + filePath);
                    }
                }
            }

            return files;

        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     *
     * @return true if upload succeeded
     */
    private static boolean uploadShard(String localShardDir,
                                        String shardName,
                                        String indexName,
                                        Path finalIndexPath,
                                        FileSystem finalFS,
                                        boolean qaMode) {
        final Path finalIndexDirPath = new Path(finalIndexPath, indexName);
        final Path finalShardPath = new Path(finalIndexDirPath, shardName + ".sqar");
        try {
            if(!finalFS.exists(finalIndexDirPath)) {
                finalFS.mkdirs(finalIndexDirPath);
                if(qaMode) {
                    makeWorldWritable(finalFS, finalIndexDirPath);
                }
            }

            if(finalFS.exists(finalShardPath)) {
                log.info("File already exists. HDFS upload aborted.");
                return true;
            }
            
            final String scheme = finalFS.getUri().getScheme();
            if (scheme.equals("hdfs")) {
                /* 
                 * upload to temp file then rename, 
                 * to avoid having other systems see a partial file
                 */
                final String tmpUploadShardName = indexName + "-" + shardName;
                final Path tempUploadPath =
                        new Path(new Path("/tmp/"), tmpUploadShardName + ".sqar");
                final File shardDir = new File(localShardDir, shardName);
                final SquallArchiveWriter writer =
                        new SquallArchiveWriter(finalFS, tempUploadPath, true,
                                                SquallArchiveCompressor.GZIP);
                writer.batchAppendDirectory(shardDir);
                writer.commit();
                finalFS.rename(tempUploadPath, finalShardPath);
            } else if (scheme.equals("s3n") || scheme.equals("s3a")) {
                /* 
                 * s3 files are only visible after the upload is complete,
                 * so no need to use a temp file
                 */
                final File shardDir = new File(localShardDir, shardName);
                final SquallArchiveWriter writer =
                        new SquallArchiveWriter(finalFS, finalShardPath, true,
                                                SquallArchiveCompressor.GZIP);
                writer.batchAppendDirectory(shardDir);
                writer.commit();
            }
        } catch (IOException e) {
            log.error(e);
            return false;
        }

        if(qaMode) {
            try {
                // try to set permissions on the uploaded file
                makeWorldWritable(finalFS, finalShardPath);
            } catch (Exception e) {
                log.warn("Failed to set permissions on the uploaded file " + finalShardPath);
            }
        }
        return true;
    }

    private static void makeWorldWritable(FileSystem fs, Path path) throws IOException {
        fs.setPermission(path, FsPermission.valueOf("-rwxrwxrwx"));
    }

    private static class FileToIndex {
        final String name;
        final String index;
        final Path fsPath;

        private FileToIndex(String name, String index, Path fsPath) {
            this.name = name;
            this.index = index;
            this.fsPath = fsPath;
        }
    }

    @SuppressWarnings("static-access")
    public static void main(String[] args) {
        final Options options;
        final CommandLineParser parser = new PosixParser();
        final CommandLine cmd;
       
        options = new Options();
        final Option idxLoc = OptionBuilder.withArgName("path")
                .hasArg()
                .withLongOpt("index-loc")
                .withDescription("")
                .isRequired()
                .create('i');
        options.addOption(idxLoc);
        final Option successLoc = OptionBuilder.withArgName("path")
                .hasArg()
                .withLongOpt("success-loc")
                .withDescription("")
                .isRequired()
                .create('s');
        options.addOption(successLoc);
        final Option failureLoc = OptionBuilder.withArgName("path")
                .hasArg()
                .withLongOpt("failure-loc")
                .withDescription("")
                .isRequired()
                .create('f');
        options.addOption(failureLoc);
        final Option dataLoc = OptionBuilder.withArgName("path")
                .hasArg()
                .withLongOpt("data-loc")
                .withDescription("Location to store the built indexes")
                .isRequired()
                .create('d');
        options.addOption(dataLoc);
        final Option buildLoc = OptionBuilder.withArgName("path")
                .hasArg()
                .withLongOpt("build-loc")
                .withDescription("Local directory were the indexes are built")
                .create('b');
        options.addOption(buildLoc);
        final Option hdfsPrincipal = OptionBuilder.withArgName("name")
                .hasArg()
                .withLongOpt("hdfs-principal")
                .withDescription("HDFS principal (only when using HDFS)")
                .create('p');
        options.addOption(hdfsPrincipal);
        final Option hdfsKeytab = OptionBuilder.withArgName("file")
                .hasArg()
                .withLongOpt("hdfs-keytab")
                .withDescription("HDFS keytab file location (only when using HDFS)")
                .create('k');
        options.addOption(hdfsKeytab);
        final Option qaMode = OptionBuilder
                .withLongOpt("qa-mode")
                .withDescription("Enable QA mode")
                .create('q');
        options.addOption(qaMode);
        final Option parallelBuilds = OptionBuilder
                .hasArg()
                .withDescription("Number of shards to build in parallel (default " + DEFAULT_PARALLEL_BUILDS + ")")
                .create("parallelbuilds");
        options.addOption(parallelBuilds);
        
        try{
            cmd = parser.parse(options, args);
        } catch( ParseException exp ) {
                System.out.println("Unexpected exception: " + exp.getMessage());
                System.out.println("\n");
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("shardBuilder", options, true);
                return;
        }
        
        /* start up the shard builder */
        final TsvConverter converter = new TsvConverter();
        converter.init(cmd.getOptionValue('i'),
                       cmd.getOptionValue('s'),
                       cmd.getOptionValue('f'),
                       cmd.getOptionValue('d'),
                       cmd.getOptionValue('b'),
                       cmd.getOptionValue('p'),
                       cmd.getOptionValue('k'),
                       cmd.hasOption('q'),
                       cmd.getOptionValue("parallel-builds", String.valueOf(DEFAULT_PARALLEL_BUILDS)) );
        
        converter.run();
    }
}

