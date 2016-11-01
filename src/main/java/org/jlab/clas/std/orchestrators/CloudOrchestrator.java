package org.jlab.clas.std.orchestrators;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.base.DpeName;
import org.jlab.clas.std.orchestrators.ReconstructionConfigParser.ConfigFileChecker;
import org.jlab.clas.std.orchestrators.ReconstructionOrchestrator.DpeCallBack;
import org.jlab.clas.std.orchestrators.errors.OrchestratorConfigError;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

public final class CloudOrchestrator extends AbstractOrchestrator {

    private final Set<ReconstructionNode> nodes = new HashSet<>();

    public static void main(String[] args) {
        try {
            CloudOrchestrator fo;
            ConfigFileChecker cc = new ConfigFileChecker(args);
            if (cc.hasFile()) {
                ConfigFileBuilder cf = new ConfigFileBuilder(cc.getFile());
                fo = cf.build();
            } else {
                CommandLineBuilder cl = new CommandLineBuilder(args);
                if (!cl.success()) {
                    System.err.printf("Usage:%n%n  cloud-orchestrator %s%n%n%n", cl.usage());
                    System.err.print(cl.help());
                    System.exit(1);
                }
                fo = cl.build();
            }
            boolean status = fo.run();
            if (status) {
                System.exit(0);
            } else {
                System.exit(1);
            }
        } catch (OrchestratorConfigError e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }


    /**
     * Helps constructing a {@link CloudOrchestrator} with all default and
     * required parameters.
     */
    public static class Builder {

        final List<ServiceInfo> recChain;
        final List<String> inputFiles;

        private DpeName frontEnd = ReconstructionConfigParser.localDpeName();
        private String session = "";
        private boolean useFrontEnd = false;
        private boolean stageFiles = false;

        private int poolSize = ReconstructionOptions.DEFAULT_POOLSIZE;
        private int maxThreads = ReconstructionOptions.MAX_THREADS;
        private int maxNodes = ReconstructionOptions.MAX_NODES;

        private String inputDir = ReconstructionPaths.INPUT_DIR;
        private String outputDir = ReconstructionPaths.OUTPUT_DIR;
        private String stageDir = ReconstructionPaths.STAGE_DIR;

        /**
         * Sets the required arguments to start a reconstruction.
         *
         * @param servicesFile the YAML file describing the reconstruction chain
         * @param inputFiles the list of files to be processed (only names).
         * @throws OrchestratorConfigError if the reconstruction chain could not be parsed
         */
        public Builder(String servicesFile, List<String> inputFiles) {
            Objects.requireNonNull(servicesFile, "servicesFile parameter is null");
            Objects.requireNonNull(inputFiles, "inputFiles parameter is null");
            if (inputFiles.isEmpty()) {
                throw new IllegalArgumentException("inputFiles list is empty");
            }
            ReconstructionConfigParser parser = new ReconstructionConfigParser(servicesFile);
            this.recChain = parser.parseReconstructionChain();
            this.inputFiles = inputFiles;
        }

        /**
         * Sets the name of the front-end. Use this if the orchestrator is not
         * running in the same node as the front-end, or if the orchestrator is
         * not using the proper network interface or port for the front-end.
         */
        public Builder withFrontEnd(DpeName frontEnd) {
            Objects.requireNonNull(frontEnd, "frontEnd parameter is null");
            this.frontEnd = frontEnd;
            return this;
        }

        /**
         * Sets the name of the front-end. Use this if the orchestrator is not
         * running in the same node as the front-end, or if the orchestrator is
         * not using the proper network interface or port for the front-end.
         */
        public Builder withSession(String session) {
            Objects.requireNonNull(session, "session parameter is null");
            this.session = session;
            return this;
        }

        /**
         * Uses the front-end for reconstruction. By default, the front-end is
         * only used for registration and discovery.
         */
        public Builder useFrontEnd() {
            this.useFrontEnd = true;
            return this;
        }

        /**
         * Stages the input file on the node for local access.
         * By default, all files are expected to be on the input directory.
         * <p>
         * When staging is used, the files will be copied on demand from the
         * input directory into the staging directory before using it.
         * The output file will also be saved in the stating directory. When the
         * reconstruction is finished, it will be moved back to the output
         * directory.
         *
         * @see #withStageDirectory(String)
         */
        public Builder useStageDirectory() {
            this.stageFiles = true;
            return this;
        }

        /**
         * Sets the size of the thread-pool that will process reports from
         * services and nodes.
         */
        public Builder withPoolSize(int poolSize) {
            if (poolSize <= 0) {
                throw new IllegalArgumentException("Invalid pool size: " + poolSize);
            }
            this.poolSize = poolSize;
            return this;
        }

        /**
         * Sets the maximum number of threads to be used for reconstruction on
         * every node.
         */
        public Builder withMaxThreads(int maxThreads) {
            if (maxThreads <= 0) {
                throw new IllegalArgumentException("Invalid max number of threads: " + maxThreads);
            }
            this.maxThreads = maxThreads;
            return this;
        }

        /**
         * Sets the maximum number of nodes to be used for reconstruction.
         */
        public Builder withMaxNodes(int maxNodes) {
            if (maxNodes <= 0) {
                throw new IllegalArgumentException("Invalid max number of nodes: " + maxNodes);
            }
            this.maxNodes = maxNodes;
            return this;
        }

        /**
         * Changes the path of the shared input directory.
         * This directory should contain all input files.
         */
        public Builder withInputDirectory(String inputDir) {
            Objects.requireNonNull(inputDir, "inputDir parameter is null");
            if (inputDir.isEmpty()) {
                throw new IllegalArgumentException("inputDir parameter is empty");
            }
            this.inputDir = inputDir;
            return this;
        }

        /**
         * Changes the path of the shared output directory.
         * This directory will contain all reconstructed output files.
         */
        public Builder withOutputDirectory(String outputDir) {
            Objects.requireNonNull(outputDir, "outputDir parameter is null");
            if (outputDir.isEmpty()) {
                throw new IllegalArgumentException("outputDir parameter is empty");
            }
            this.outputDir = outputDir;
            return this;
        }

        /**
         * Changes the path of the local staging directory.
         * Files will be staged in this directory of every worker node
         * for fast access.
         */
        public Builder withStageDirectory(String stageDir) {
            Objects.requireNonNull(stageDir, "stageDir parameter is null");
            if (stageDir.isEmpty()) {
                throw new IllegalArgumentException("stageDir parameter is empty");
            }
            this.stageDir = stageDir;
            return this;
        }

        /**
         * Creates the orchestrator.
         */
        public CloudOrchestrator build() {
            ReconstructionSetup setup = new ReconstructionSetup(recChain, frontEnd, session);
            ReconstructionPaths paths = new ReconstructionPaths(inputFiles,
                    inputDir, outputDir, stageDir);
            ReconstructionOptions options = new ReconstructionOptions(
                    useFrontEnd, stageFiles,
                    poolSize, maxNodes, maxThreads);
            return new CloudOrchestrator(setup, paths, options);
        }
    }


    private CloudOrchestrator(ReconstructionSetup setup,
                              ReconstructionPaths paths,
                              ReconstructionOptions options) {
        super(setup, paths, options);
        Logging.verbose(true);
    }


    @Override
    void start() {
        printStartup();
        Logging.info("Waiting for reconstruction nodes...");
        orchestrator.listenDpes(new DpeReportCB(), setup.session);
    }


    @Override
    void end() {
        Logging.info("Local  average event processing time = %.2f ms", stats.localAverage());
        Logging.info("Global average event processing time = %.2f ms", stats.globalAverage());
    }


    private void printStartup() {
        System.out.println("****************************************");
        System.out.println("*        CLAS Cloud Orchestrator       *");
        System.out.println("****************************************");
        System.out.println("- Front-end    = " + setup.frontEnd);
        System.out.println("- Start time   = " + ClaraUtil.getCurrentTime());
        System.out.println("- Pool size    = " + options.poolSize);
        System.out.println();
        System.out.println("- Input directory  = " + paths.inputDir);
        System.out.println("- Output directory = " + paths.outputDir);
        if (options.stageFiles) {
            System.out.println("- Stage directory  = " + paths.stageDir);
        }
        System.out.println("- Number of files  = " + paths.numFiles());
        System.out.println("****************************************");
    }



    private class DpeReportCB implements DpeCallBack {

        @Override
        public void callback(DpeInfo dpe) {
            final ReconstructionNode node = new ReconstructionNode(orchestrator, dpe);
            synchronized (nodes) {
                if (nodes.size() == options.maxNodes || filterNode(node)) {
                    return;
                }
                if (!nodes.contains(node)) {
                    try {
                        executeSetup(node);
                        nodes.add(node);
                    } catch (RejectedExecutionException e) {
                        // ignore
                    }
                }
            }
        }

        private boolean filterNode(ReconstructionNode node) {
            DpeName name = node.dpe.name;
            if (name.equals(setup.frontEnd) && !options.useFrontEnd) {
                return true;
            }
            return false;
        }
    }


    public static class CommandLineBuilder {

        private static final String ARG_FRONTEND      = "frontEnd";
        private static final String ARG_SESSION       = "session";
        private static final String ARG_USE_FRONTEND  = "useFrontEnd";
        private static final String ARG_STAGE_FILES   = "stageFiles";
        private static final String ARG_CACHE_DIR     = "cacheDir";
        private static final String ARG_INPUT_DIR     = "inputDir";
        private static final String ARG_OUTPUT_DIR    = "outputDir";
        private static final String ARG_STAGE_DIR     = "stageDir";
        private static final String ARG_POOL_SIZE     = "poolSize";
        private static final String ARG_MAX_NODES     = "maxNodes";
        private static final String ARG_MAX_THREADS   = "maxThreads";
        private static final String ARG_SERVICES_FILE = "servicesFile";
        private static final String ARG_INPUT_FILES   = "inputFiles";

        private final JSAP jsap;
        private final JSAPResult config;

        public CommandLineBuilder(String[] args) {
            jsap = new JSAP();
            setArguments(jsap);
            config = jsap.parse(args);
        }

        public boolean success() {
            return config.success();
        }

        public String usage() {
            return jsap.getUsage();
        }

        public String help() {
            return jsap.getHelp();
        }

        public CloudOrchestrator build() {
            DpeName frontEnd = parseFrontEnd();
            String session = parseSession();
            boolean useFrontEnd = config.getBoolean(ARG_USE_FRONTEND);

            int poolSize = config.getInt(ARG_POOL_SIZE);
            int maxNodes = config.getInt(ARG_MAX_NODES);
            int maxThreads = config.getInt(ARG_MAX_THREADS);

            String services = config.getString(ARG_SERVICES_FILE);
            String files = config.getString(ARG_INPUT_FILES);
            boolean stageFiles = config.getBoolean(ARG_STAGE_FILES);

            String inDir = config.getString(ARG_INPUT_DIR);
            String outDir = config.getString(ARG_OUTPUT_DIR);
            String tmpDir = config.getString(ARG_STAGE_DIR);

            ReconstructionConfigParser parser = new ReconstructionConfigParser(services);
            List<ServiceInfo> recChain = parser.parseReconstructionChain();
            List<String> inFiles = parser.readInputFiles(files);

            ReconstructionSetup setup = new ReconstructionSetup(recChain, frontEnd, session);
            ReconstructionPaths paths = new ReconstructionPaths(inFiles, inDir, outDir, tmpDir);
            ReconstructionOptions options = new ReconstructionOptions(
                    useFrontEnd, stageFiles,
                    poolSize, maxNodes, maxThreads);

            return new CloudOrchestrator(setup, paths, options);
        }

        private DpeName parseFrontEnd() {
            String frontEnd = config.getString(ARG_FRONTEND)
                                    .replaceFirst("localhost", ClaraUtil.localhost());
            try {
                return new DpeName(frontEnd);
            } catch (IllegalArgumentException e) {
                throw new OrchestratorConfigError("invalid front-end name: " + frontEnd);
            }
        }

        private String parseSession() {
            String session = config.getString(ARG_SESSION);
            if (session == null) {
                return "";
            }
            return session;
        }

        private void setArguments(JSAP jsap) {
            FlaggedOption frontEnd = new FlaggedOption(ARG_FRONTEND)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(false)
                    .setShortFlag('f')
                    .setDefault(ReconstructionConfigParser.localDpeName().toString());
            frontEnd.setHelp("The name of the CLARA front-end DPE.");

            FlaggedOption session = new FlaggedOption(ARG_SESSION)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setShortFlag('s')
                    .setRequired(false);
            session.setHelp("The session of the CLARA DPEs to be used for reconstruction.");

            Switch useFrontEnd = new Switch(ARG_USE_FRONTEND)
                    .setShortFlag('F');
            useFrontEnd.setHelp("Use front-end for reconstruction.");

            Switch stageFiles = new Switch(ARG_STAGE_FILES)
                    .setShortFlag('L');
            stageFiles.setHelp("Stage files to the local file-system before using them.");

            FlaggedOption tapeDir = new FlaggedOption(ARG_CACHE_DIR)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(false)
                    .setShortFlag('c')
                    .setDefault(ReconstructionPaths.CACHE_DIR);
            tapeDir.setHelp("The tape directory where the files are cached.");

            FlaggedOption inputDir = new FlaggedOption(ARG_INPUT_DIR)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(false)
                    .setShortFlag('i')
                    .setDefault(ReconstructionPaths.INPUT_DIR);
            inputDir.setHelp("The input directory where the files to be processed are located.");

            FlaggedOption outputDir = new FlaggedOption(ARG_OUTPUT_DIR)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(false)
                    .setShortFlag('o')
                    .setDefault(ReconstructionPaths.OUTPUT_DIR);
            outputDir.setHelp("The output directory where reconstructed files will be saved.");

            FlaggedOption stageDir = new FlaggedOption(ARG_STAGE_DIR)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(false)
                    .setShortFlag('l')
                    .setDefault(ReconstructionPaths.STAGE_DIR);
            stageDir.setHelp("The local stage directory where the temporary files will be stored.");

            FlaggedOption poolSize = new FlaggedOption(ARG_POOL_SIZE)
                    .setStringParser(JSAP.INTEGER_PARSER)
                    .setShortFlag('p')
                    .setDefault(String.valueOf(ReconstructionOptions.DEFAULT_POOLSIZE))
                    .setRequired(false);
            poolSize.setHelp("The size of the thread-pool processing service and node reports.");

            FlaggedOption maxNodes = new FlaggedOption(ARG_MAX_NODES)
                    .setStringParser(JSAP.INTEGER_PARSER)
                    .setShortFlag('n')
                    .setDefault(String.valueOf(ReconstructionOptions.MAX_NODES))
                    .setRequired(false);
            maxNodes.setHelp("The maximum number of reconstruction nodes to be used.");

            FlaggedOption maxThreads = new FlaggedOption(ARG_MAX_THREADS)
                    .setStringParser(JSAP.INTEGER_PARSER)
                    .setShortFlag('t')
                    .setDefault(String.valueOf(ReconstructionOptions.MAX_THREADS))
                    .setRequired(false);
            maxThreads.setHelp("The maximum number of reconstruction threads to be used per node.");

            UnflaggedOption servicesFile = new UnflaggedOption(ARG_SERVICES_FILE)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(true);
            servicesFile.setHelp("The YAML file with the reconstruction chain description.");

            UnflaggedOption inputFiles = new UnflaggedOption(ARG_INPUT_FILES)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(true);
            inputFiles.setHelp("The file with the list of input files to be reconstructed"
                              + " (one name per line).");

            try {
                jsap.registerParameter(frontEnd);
                jsap.registerParameter(session);
                jsap.registerParameter(useFrontEnd);
                jsap.registerParameter(stageFiles);
                jsap.registerParameter(inputDir);
                jsap.registerParameter(outputDir);
                jsap.registerParameter(stageDir);
                jsap.registerParameter(poolSize);
                jsap.registerParameter(maxNodes);
                jsap.registerParameter(maxThreads);
                jsap.registerParameter(servicesFile);
                jsap.registerParameter(inputFiles);
            } catch (JSAPException e) {
                throw new RuntimeException(e);
            }
        }
    }


    public static class ConfigFileBuilder {

        private final String configFile;

        public ConfigFileBuilder(String configFile) {
            this.configFile = configFile;
        }

        public CloudOrchestrator build() {
            ReconstructionConfigParser parser = new ReconstructionConfigParser(configFile);

            DpeName frontEnd = ReconstructionConfigParser.localDpeName();
            List<ServiceInfo> recChain = parser.parseReconstructionChain();
            List<String> inFiles = parser.readInputFiles();

            ReconstructionSetup setup = new ReconstructionSetup(recChain, frontEnd, "");
            ReconstructionPaths paths = new ReconstructionPaths(inFiles,
                                              parser.parseDirectory("input"),
                                              parser.parseDirectory("output"),
                                              parser.parseDirectory("tmp"));
            ReconstructionOptions options = new ReconstructionOptions();
            return new CloudOrchestrator(setup, paths, options);
        }
    }
}
