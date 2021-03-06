package org.jlab.clas.std.orchestrators;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.jlab.clara.base.ClaraLang;
import org.jlab.clara.base.DpeName;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.ClaraSerializer;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clas.std.orchestrators.errors.OrchestratorConfigError;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.hipo.data.HipoEvent;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.yaml.snakeyaml.Yaml;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;

/**
 * Helper class to read configuration for the standard orchestrators.
 * <p>
 * Currently, the user can set:
 * <ul>
 * <li>The list of services in the reconstruction chain
 * <li>The name of the container for the services
 * <li>The list of I/O and reconstruction nodes
 * <li>The list of input files
 * </ul>
 *
 * The <i>reconstruction services</i> description is provided in a YAML file,
 * which format is the following:
 * <pre>
 * container: my-default # Optional: change default container, otherwise it is $USER
 * services:
 *   - class: org.jlab.clas12.ana.serviceA
 *     name: serviceA
 *   - class: org.jlab.clas12.rec.serviceB
 *     name: serviceB
 *     container: containerB # Optional: change container for this service
 * </pre>
 * By default, all reconstruction and I/O services will be deployed in a
 * container named as the {@code $USER} running the orchestrator. This can be
 * changed by including a {@code container} key with the desired container name.
 * The container can be overwritten for individual services too. There is no
 * need to include I/O services in this file. They are controlled by the
 * orchestrators.
 * <p>
 *
 * The <i>nodes</i> description is also provided as a YAML file, with the
 * following format:
 * <pre>
 * input-output:
 *   - name: io-node
 * reconstruction:
 *   - name: rec-node-1
 *     cores: 12
 *   - name: rec-node-2
 *     cores: 32
 *   - name: rec-node-3
 * </pre>
 * Note that the {@code clara-services} and {@code cores} support is experimental.
 *
 * The <i>input files</i> description is just a simple text file with the list
 * of all the input files, one per line:
 * <pre>
 * input-file1.ev
 * input-file2.ev
 * input-file3.ev
 * input-file4.ev
 * </pre>
 */
public class ReconstructionConfigParser {

    private static final String DEFAULT_CONTAINER = System.getProperty("user.name");

    private final JSONObject config;


    public ReconstructionConfigParser(String configFilePath) {
        try (InputStream input = new FileInputStream(configFilePath)) {
            Yaml yaml = new Yaml();
            @SuppressWarnings("unchecked")
            Map<String, Object> config = (Map<String, Object>) yaml.load(input);
            this.config = new JSONObject(config);
        } catch (IOException e) {
            throw error(e);
        }
    }


    public static String getDefaultContainer() {
        return DEFAULT_CONTAINER;
    }


    public static ServiceInfo ioServiceFactory(String className, String engineName) {
        return new ServiceInfo(className, getDefaultContainer(), engineName, ClaraLang.JAVA);
    }


    private static ServiceInfo defaultIOService(String service, String dataFormat) {
        // CHECKSTYLE.OFF: Indentation
        switch (dataFormat) {
        case "evio":
            switch (service) {
            case "reader":
                return ioServiceFactory("org.jlab.clas.std.services.convertors.EvioToEvioReader",
                                        "EvioToEvioReader");
            case "writer":
                return ioServiceFactory("org.jlab.clas.std.services.convertors.EvioToEvioWriter",
                                        "EvioToEvioWriter");
            default:
                throw error("Invalid IO service key: " + dataFormat);
            }
        case "hipo":
            switch (service) {
            case "reader":
                return ioServiceFactory("org.jlab.clas.std.services.convertors.HipoToHipoReader",
                                        "HipoToHipoReader");
            case "writer":
                return ioServiceFactory("org.jlab.clas.std.services.convertors.HipoToHipoWriter",
                                        "HipoToHipoWriter");
            default:
                throw error("Invalid IO service key: " + dataFormat);
            }
        default:
            throw error("Unsupported default data format: " + dataFormat);
        }
        // CHECKSTYLE.ON: Indentation
    }


    private static Map<String, ServiceInfo> defaultIOServices(String dataFormat) {
        Map<String, ServiceInfo> services = new HashMap<>();
        services.put("reader", defaultIOService("reader", dataFormat));
        services.put("writer", defaultIOService("writer", dataFormat));

        ServiceInfo stage = ioServiceFactory("org.jlab.clas.std.services.system.DataManager",
                                             "DataManager");
        services.put("stage", stage);

        return services;
    }


    private static Set<EngineDataType> defaultDataTypes(String dataFormat) {
        // TODO: CLAS12 base package should provide these types
        Set<EngineDataType> dt = new HashSet<>();
        if (dataFormat.equals("evio")) {
            dt.add(new EngineDataType("binary/data-evio", EngineDataType.BYTES.serializer()));
        } else if (dataFormat.equals("hipo")) {
            dt.add(new EngineDataType("binary/data-hipo", new ClaraSerializer() {
                @Override
                public ByteBuffer write(Object data) throws ClaraException {
                    HipoEvent event = (HipoEvent) data;
                    return ByteBuffer.wrap(event.getDataBuffer());
                }

                @Override
                public Object read(ByteBuffer buffer) throws ClaraException {
                    return new HipoEvent(buffer.array());
                }
            }));
        } else {
            throw error("Invalid data format: " + dataFormat);
        }
        return dt;
    }


    public Set<EngineDataType> parseDataTypes() {
        Set<EngineDataType> dt = defaultDataTypes("evio");
        JSONObject io = config.optJSONObject("io-services");
        if (io != null) {
            String dataFormat = io.optString("use");
            if (!dataFormat.isEmpty()) {
                dt.clear();
                dt.addAll(defaultDataTypes(dataFormat));
            }
            Consumer<String> getTypes = key -> {
                String f = io.optString(key);
                if (!f.isEmpty()) {
                    dt.addAll(defaultDataTypes(f));
                }
            };
            getTypes.accept("reader");
            getTypes.accept("writer");
        }
        return dt;
    }


    public Map<String, ServiceInfo> parseInputOutputServices() {
        Map<String, ServiceInfo> services = defaultIOServices("evio");
        JSONObject io = config.optJSONObject("io-services");
        if (io != null) {
            String dataFormat = io.optString("use");
            if (!dataFormat.isEmpty()) {
                services.putAll(defaultIOServices(dataFormat));
            }
            Consumer<String> getTypes = key -> {
                if (!io.has(key)) {
                    return;
                }
                String f = parseIOFormat(io, key);
                if (f != null) {
                    services.put(key, defaultIOService(key, f));
                    return;
                }
                JSONObject data = io.optJSONObject(key);
                if (data != null) {
                    services.put(key, parseService(data));
                }
            };
            getTypes.accept("reader");
            getTypes.accept("writer");
        }
        return services;
    }


    public List<ServiceInfo> parseReconstructionChain() {
        List<ServiceInfo> services = new ArrayList<ServiceInfo>();
        JSONArray sl = config.optJSONArray("services");
        if (sl == null) {
            throw error("missing list of services");
        }
        for (int i = 0; i < sl.length(); i++) {
            ServiceInfo service = parseService(sl.getJSONObject(i));
            if (services.contains(service)) {
                throw error(String.format("duplicated service  name = '%s' container = '%s'",
                                          service.name, service.cont));
            }
            services.add(service);
        }
        return services;
    }


    public JSONObject parseReconstructionConfig() {
        if (config.has("configuration")) {
            return config.getJSONObject("configuration");
        }
        return new JSONObject();
    }


    private String parseDefaultContainer() {
        return config.optString("container", DEFAULT_CONTAINER);
    }


    private String parseDefaultLanguage() {
        return config.optString("lang", ClaraLang.JAVA.toString());
    }


    private String parseIOFormat(JSONObject data, String key) {
        try {
            return data.getString(key);
        } catch (JSONException e) {
            // not a string
            return null;
        }
    }


    private ServiceInfo parseService(JSONObject data) {
        String name = data.optString("name");
        String classPath = data.optString("class");
        String container = data.optString("container", parseDefaultContainer());
        ClaraLang lang = ClaraLang.fromString(data.optString("lang", parseDefaultLanguage()));
        if (name.isEmpty() || classPath.isEmpty()) {
            throw error("missing name or class of service");
        }
        return new ServiceInfo(classPath, container, name, lang);
    }


    public List<DpeInfo> parseReconstructionNodes() {
        return parseNodes("reconstruction");
    }


    public List<DpeInfo> parseInputOutputNodes() {
        return parseNodes("input-output");
    }


    private List<DpeInfo> parseNodes(String nodeType) {
        List<DpeInfo> dpes = new ArrayList<DpeInfo>();
        JSONArray dl = config.optJSONArray(nodeType);
        if (dl == null) {
            throw error("missing list of " + nodeType + " nodes");
        }
        String defaultClaraHome = config.optString("clara-home", DpeInfo.DEFAULT_CLARA_HOME);
        for (int i = 0; i < dl.length(); i++) {
            JSONObject d = dl.getJSONObject(i);
            String address = d.optString("name");
            if (address.isEmpty()) {
                throw error("missing name of " + nodeType + " node");
            }
            DpeName name = new DpeName(hostAddress(address), ClaraLang.JAVA);
            int cores = d.optInt("cores", 0);
            String claraHome = d.optString("clara-home", defaultClaraHome);
            dpes.add(new DpeInfo(name, cores, claraHome));
        }
        return dpes;
    }


    public static DpeInfo getDefaultDpeInfo(String hostName) {
        String dpeIp = hostAddress(hostName);
        DpeName dpeName = new DpeName(dpeIp, ClaraLang.JAVA);
        return new DpeInfo(dpeName, 0, DpeInfo.DEFAULT_CLARA_HOME);
    }


    public static DpeName localDpeName() {
        return new DpeName(hostAddress("localhost"), ClaraLang.JAVA);
    }


    public static String hostAddress(String host) {
        try {
            return xMsgUtil.toHostAddress(host);
        } catch (UncheckedIOException e) {
            throw error("node name not known: " + host);
        }
    }


    public String parseInputFile() {
        String inputFile = config.optString("input-file");
        if (inputFile.isEmpty()) {
            throw error("missing input file");
        }
        return inputFile;
    }


    public String parseOutputFile() {
        String outputFile = config.optString("output-file");
        if (outputFile.isEmpty()) {
            throw error("missing output file");
        }
        return outputFile;
    }


    public int parseNumberOfThreads() {
        String key = "threads";
        if (!config.has(key)) {
            throw error("missing number of threads");
        }
        int nt = config.optInt(key);
        if (nt <= 0) {
            throw error(String.format("invalid number of threads"));
        }
        return nt;
    }


    public String parseDirectory(String key) {
        JSONObject dirsConfig = config.optJSONObject("dirs");
        if (dirsConfig == null) {
            throw error("missing directories configuration");
        }
        String dir = dirsConfig.optString(key);
        if (dir.isEmpty()) {
            throw error("missing directory path: " + key);
        }
        return dir;
    }


    public List<String> readInputFiles(String inputFilesList) {
        try {
            Pattern pattern = Pattern.compile("^\\s*#.*$");
            return Files.lines(Paths.get(inputFilesList))
                        .filter(line -> !line.isEmpty())
                        .filter(line -> !pattern.matcher(line).matches())
                        .collect(Collectors.toList());
        } catch (IOException e) {
            throw error("Could not read file " + inputFilesList);
        }
    }


    public List<String> readInputFiles() {
        JSONArray files = config.optJSONArray("files");
        if (files == null) {
            throw error("missing list of files");
        }
        try {
            return IntStream.range(0, files.length())
                            .mapToObj(files::getString)
                            .collect(Collectors.toList());
        } catch (JSONException e) {
            throw error("invalid list of files");
        }
    }


    public int parseProcessingTimes() {
        JSONObject runConfig = config.optJSONObject("run");
        if (runConfig == null) {
            throw error("missing runtime configuration");
        }
        String key = "times";
        if (!config.has(key)) {
            throw error("missing processing times number");
        }
        int times = config.optInt(key);
        if (times <= 0) {
            throw error(String.format("invalid number of processing times"));
        }
        return times;
    }


    private static OrchestratorConfigError error(String msg) {
        return new OrchestratorConfigError(msg);
    }


    private static OrchestratorConfigError error(Throwable cause) {
        return new OrchestratorConfigError(cause);
    }


    public static class ConfigFileChecker {
        private final JSAP jsap;
        private final JSAPResult config;

        public ConfigFileChecker(String[] args) {
            jsap = new JSAP();
            setArguments(jsap);
            config = jsap.parse(args);
        }

        public boolean hasFile() {
            return config.success();
        }

        public String getFile() {
            return config.getString(ARG_CONFIG_FILE);
        }

        private static final String ARG_CONFIG_FILE = "full_config";

        private void setArguments(JSAP jsap) {

            FlaggedOption configFileArg = new FlaggedOption(ARG_CONFIG_FILE)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(true)
                    .setShortFlag('f');
            configFileArg.setHelp("The full configuration file");

            try {
                jsap.registerParameter(configFileArg);
            } catch (JSAPException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
