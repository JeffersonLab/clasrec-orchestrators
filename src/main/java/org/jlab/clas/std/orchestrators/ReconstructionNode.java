package org.jlab.clas.std.orchestrators;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jlab.clara.base.Composition;
import org.jlab.clara.base.ContainerName;
import org.jlab.clara.base.DpeName;
import org.jlab.clara.base.ServiceName;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.engine.EngineStatus;
import org.jlab.clara.util.CConstants;
import org.jlab.clas.std.orchestrators.errors.OrchestratorError;
import org.jlab.clas12.tools.MimeType;
import org.jlab.clas12.tools.property.JPropertyList;


class ReconstructionNode {

    private final ReconstructionOrchestrator orchestrator;

    final DpeInfo dpe;
    final ContainerName containerName;
    final ServiceName stageName;
    final ServiceName readerName;
    final ServiceName writerName;

    String currentInputFileName;
    String currentInputFile;
    String currentOutputFile;

    int eventNumber;
    long startTime;


    ReconstructionNode(ReconstructionOrchestrator orchestrator, DpeInfo dpe) {
        if (orchestrator == null) {
            throw new IllegalArgumentException("Null orchestrator parameter");
        }
        if (dpe == null) {
            throw new IllegalArgumentException("Null DPE parameter");
        }

        this.dpe = dpe;
        this.orchestrator = orchestrator;

        this.containerName = new ContainerName(dpe.name,
                                               ReconstructionConfigParser.getDefaultContainer());
        this.stageName = orchestrator.getStageServiceName(dpe);
        this.readerName = orchestrator.getReaderServiceName(dpe);
        this.writerName = orchestrator.getWriterServiceName(dpe);
    }


    void setPaths(String inputPath, String outputPath, String stagePath) {
        JPropertyList pl = new JPropertyList();
        pl.addTailProperty("input_path", inputPath);
        pl.addTailProperty("output_path", outputPath);
        pl.addTailProperty("stage_path", stagePath);
        syncConfig(stageName, pl, 120);
    }


    boolean setFiles(String inputFileName) {
        try {
            currentInputFileName = inputFileName;
            currentInputFile = CConstants.UNDEFINED;
            currentOutputFile = CConstants.UNDEFINED;

            JPropertyList pl = new JPropertyList();
            pl.addHeadProperty("action", "stage_input");
            pl.addTailProperty("file", currentInputFileName);

            Logging.info("Staging file %s in %s%n", currentInputFileName, dpe.name);
            EngineData result = syncSend(stageName, pl, 300);

            if (!result.getStatus().equals(EngineStatus.ERROR)) {
                JPropertyList rl = (JPropertyList) result.getData();
                currentInputFile = rl.getPropertyValue("input_file");
                currentOutputFile = rl.getPropertyValue("output_file");
                return true;
            } else {
                System.err.println(result.getDescription());
                currentInputFileName = CConstants.UNDEFINED;
                return false;
            }
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not configure directories", e);
        }

    }


    void setFiles(String inputFile, String outputFile) {
        currentInputFile = inputFile;
        currentOutputFile = outputFile;
        currentInputFileName = new File(inputFile).getName();
    }


    boolean saveOutputFile() {
        try {
            JPropertyList plr = new JPropertyList();
            plr.addHeadProperty("action", "remove_input");
            plr.addTailProperty("file", currentInputFileName);
            EngineData rr = syncSend(stageName, plr, 300);

            JPropertyList pls = new JPropertyList();
            pls.addHeadProperty("action", "save_output");
            pls.addTailProperty("file", currentInputFileName);
            EngineData rs = syncSend(stageName, pls, 300);

            currentInputFileName = CConstants.UNDEFINED;
            currentInputFile = CConstants.UNDEFINED;
            currentOutputFile = CConstants.UNDEFINED;

            boolean status = true;
            if (rr.getStatus().equals(EngineStatus.ERROR)) {
                System.err.println(rr.getDescription());
                status = false;
            }
            if (rs.getStatus().equals(EngineStatus.ERROR)) {
                status = false;
                System.err.println(rs.getDescription());
            }

            return status;
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not save output", e);
        }
    }


    void openFiles() {
        openFiles(0);

        // total number of events in the file
        eventNumber = requestNumberOfEvents();
    }


    void openFiles(int frequency) {
        startTime = 0;
        eventNumber = 0;

        // open input file
        Logging.info("Opening file %s in %s", currentInputFileName, dpe.name);
        JPropertyList inputConfig = new JPropertyList();
        inputConfig.addHeadProperty("action", "open");
        inputConfig.addTailProperty("file", currentInputFile);
        syncConfig(readerName, inputConfig, 30);

        // endiannes of the file
        String fileOrder = requestFileOrder();

        // open output file
        JPropertyList outputConfig = new JPropertyList();
        outputConfig.addHeadProperty("action", "open");
        outputConfig.addTailProperty("file", currentOutputFile);
        outputConfig.addTailProperty("order", fileOrder);
        outputConfig.addTailProperty("overwrite", "true");
        syncConfig(writerName, outputConfig, 30);

        // set "report done" frequency
        if (frequency <= 0) {
            return;
        }
        try {
            orchestrator.base.configure(writerName).startDoneReporting(frequency).run();
        } catch (ClaraException e) {
            throw new OrchestratorError("Could not configure service = " + writerName, e);
        }
    }


    void closeFiles() {
        JPropertyList plr = new JPropertyList();
        plr.addHeadProperty("action", "close");
        plr.addTailProperty("file", currentInputFile);
        syncConfig(readerName, plr, 30);

        JPropertyList plw = new JPropertyList();
        plw.addHeadProperty("action", "close");
        plw.addTailProperty("file", currentOutputFile);
        syncConfig(writerName, plr, 30);
    }


    private String requestFileOrder() {
        try {
            EngineData output = syncSend(readerName, "order", 30);
            return (String) output.getData();
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not get input file order", e);
        }
    }


    private int requestNumberOfEvents() {
        try {
            EngineData output = syncSend(readerName, "count", 30);
            return (Integer) output.getData();
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not get number of input events", e);
        }
    }


    void configureService(ServiceName service, EngineData data) {
        try {
            orchestrator.base.configure(service)
                             .withData(data)
                             .syncRun(2, TimeUnit.MINUTES);
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not configure " + service, e);
        }
    }


    void sendEventsToDpe(DpeName dpeName, List<ServiceName> chain, int dpeCores) {
        if (startTime == 0) {
            startTime = System.currentTimeMillis();
        }

        int requestId = 1;
        if (eventNumber > 0) {
            Logging.info("Using %d cores in %s to reconstruct %d events of %s",
                         dpeCores, dpeName, eventNumber, currentInputFileName);
        } else {
            Logging.info("Using %d cores in %s to reconstruct %s",
                         dpeCores, dpeName, currentInputFileName);
        }

        for (int i = 0; i < dpeCores; i++) {
            requestEvent(dpeName, chain, requestId++, "next");
        }
    }


    void requestEvent(DpeName dpeName, List<ServiceName> chain, int requestId, String type) {
        try {
            EngineData data = new EngineData();
            data.setData(EngineDataType.STRING.mimeType(), type);
            data.setCommunicationId(requestId);
            Composition composition = generateComposition(chain);
            orchestrator.base.execute(composition).withData(data).run();
        } catch (ClaraException e) {
            throw new OrchestratorError("Could not request reconstruction on = " + dpeName, e);
        }
    }


    private Composition generateComposition(List<ServiceName> chain) {
        String composition = readerName.canonicalName();
        for (ServiceName service : chain) {
            composition += "+" + service.canonicalName();
        }
        composition += "+" + writerName.canonicalName();
        composition += "+" + readerName.canonicalName();
        return new Composition(composition);
    }


    private void syncConfig(ServiceName serviceName, JPropertyList data, int timeout) {
        try {
            EngineData input = new EngineData();
            input.setData(MimeType.PROPERTY_LIST.type(), data);
            orchestrator.base.configure(serviceName)
                             .withData(input)
                             .syncRun(timeout, TimeUnit.SECONDS);
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not configure service = " + serviceName, e);
        }
    }


    private EngineData syncSend(ServiceName serviceName, String data, int timeout)
            throws ClaraException, TimeoutException {
        EngineData input = new EngineData();
        input.setData(EngineDataType.STRING.mimeType(), data);
        return syncSend(serviceName, input, timeout);
    }


    private EngineData syncSend(ServiceName serviceName, JPropertyList data, int timeout)
            throws ClaraException, TimeoutException {
        EngineData input = new EngineData();
        input.setData(MimeType.PROPERTY_LIST.type(), data);
        return syncSend(serviceName, input, timeout);
    }


    private EngineData syncSend(ServiceName serviceName, EngineData input, int timeout)
            throws ClaraException, TimeoutException {
        EngineData output = orchestrator.base.execute(serviceName)
                                             .withData(input)
                                             .syncRun(30, TimeUnit.SECONDS);
        if (output.getStatus() == EngineStatus.ERROR) {
            throw new ClaraException(output.getDescription());
        }
        return output;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + dpe.hashCode();
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof ReconstructionNode)) {
            return false;
        }
        ReconstructionNode other = (ReconstructionNode) obj;
        if (!dpe.equals(other.dpe)) {
            return false;
        }
        return true;
    }
}
