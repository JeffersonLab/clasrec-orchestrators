package org.jlab.clas.std.orchestrators;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import org.jlab.clara.base.BaseOrchestrator;
import org.jlab.clara.base.ClaraLang;
import org.jlab.clara.base.ClaraName;
import org.jlab.clara.base.ContainerName;
import org.jlab.clara.base.DpeName;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.base.EngineCallback;
import org.jlab.clara.base.GenericCallback;
import org.jlab.clara.base.ServiceName;
import org.jlab.clara.engine.ClaraSerializer;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.engine.EngineStatus;
import org.jlab.clara.util.CConstants;
import org.jlab.clas.std.orchestrators.errors.OrchestratorError;
import org.jlab.clas12.tools.MimeType;
import org.jlab.clas12.tools.property.JPropertyList;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegResponse;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

class ReconstructionOrchestrator {

    final BaseOrchestrator base;
    final String feHost;

    private ServiceInfo stage;
    private ServiceInfo reader;
    private ServiceInfo writer;

    private List<ServiceInfo> reconstructionChain;
    private Set<ContainerName> userContainers;
    private Map<ServiceName, DeployedService> userServices;

    private final ZContext context = new ZContext();

    ReconstructionOrchestrator() throws ClaraException, IOException {
        this(ReconstructionConfigParser.hostAddress("localhost"), 2);
    }

    ReconstructionOrchestrator(String frontEnd, int poolSize) throws ClaraException, IOException {
        base = new BaseOrchestrator(new DpeName(frontEnd, ClaraLang.JAVA), poolSize);
        feHost = frontEnd;
        userContainers = Collections.newSetFromMap(new ConcurrentHashMap<ContainerName, Boolean>());
        userServices = new ConcurrentHashMap<>();
        setInputOutputServices();
        registerDataTypes();
    }


    void setReconstructionChain(List<ServiceInfo> reconstructionChain) {
        if (reconstructionChain == null) {
            throw new IllegalArgumentException("null reconstruction chain");
        }
        if (reconstructionChain.isEmpty()) {
            throw new IllegalArgumentException("empty reconstruction chain");
        }
        this.reconstructionChain = reconstructionChain;
    }


    BaseOrchestrator base() {
        return base;
    }


    private void setInputOutputServices() {
        String stageClass = "org.jlab.clas.std.services.system.DataManager";
        String stageContainer = ReconstructionConfigParser.getDefaultContainer();
        String stageName = "DataManager";

        String readerClass = "org.jlab.clas.std.services.convertors.EvioToEvioReader";
        String readerContainer = ReconstructionConfigParser.getDefaultContainer();
        String readerName = "EvioToEvioReader";

        String writerClass = "org.jlab.clas.std.services.convertors.EvioToEvioWriter";
        String writerContainer = ReconstructionConfigParser.getDefaultContainer();
        String writerName = "EvioToEvioWriter";

        stage = new ServiceInfo(stageClass, stageContainer, stageName);
        reader = new ServiceInfo(readerClass, readerContainer, readerName);
        writer = new ServiceInfo(writerClass, writerContainer, writerName);
    }


    // TODO: CLAS12 base package should provide these types
    private void registerDataTypes() {
        EngineDataType propertyList = new EngineDataType(MimeType.PROPERTY_LIST.type(),
                new ClaraSerializer() {

                    @Override
                    public ByteBuffer write(Object data) throws ClaraException {
                        JPropertyList pl = (JPropertyList) data;
                        return ByteBuffer.wrap(pl.getStringRepresentation(true).getBytes());
                    }


                    @Override
                    public Object read(ByteBuffer buffer) throws ClaraException {
                        return new JPropertyList(new String(buffer.array()));
                    }
                });

        EngineDataType evio =
                new EngineDataType(MimeType.EVIO.type(), EngineDataType.BYTES.serializer());

        base.registerDataTypes(propertyList,
                               evio,
                               EngineDataType.STRING,
                               EngineDataType.SFIXED32);
    }


    private void deploy(ServiceInfo service, DpeInfo dpe, int poolsize) {
        try {
            ContainerName containerName = new ContainerName(dpe.name, service.cont);
            if (!userContainers.contains(containerName)) {
                deployContainer(dpe, service.cont);
                userContainers.add(containerName);
            }
            ServiceName serviceName = new ServiceName(containerName, service.name);
            base.deploy(serviceName, service.classpath).withPoolsize(poolsize).run();
            userServices.put(serviceName, new DeployedService(service, dpe, poolsize));
        } catch (ClaraException e) {
            String errorMsg = String.format("failed request to deploy host = '%s' "
                                            + "container = '%s' classpath = '%s'",
                                            dpe.name, service.cont, service.classpath);
            throw new OrchestratorError(errorMsg, e);
        }
    }


    private void deployContainer(DpeInfo dpe, String containerName)
            throws ClaraException {

        ContainerName container = new ContainerName(dpe.name, containerName);
        base.deploy(container).run();

        final int maxAttempts = 10;
        int counter = 0;
        while (true) {
            Set<ContainerName> regContainers = getRegisteredContainers(dpe);
            for (ContainerName c : regContainers) {
                if (container.equals(c)) {
                    return;
                }
            }
            counter++;
            if (counter == 6) {
                base.deploy(container).run();
            }
            if (counter == maxAttempts) {
                throw new OrchestratorError("could not start container = " + container);
            }
            sleep(200);
        }
    }


    void deployInputOutputServices(DpeInfo dpe, int poolsize) {
        deploy(stage, dpe, poolsize);
        deploy(reader, dpe, poolsize);
        deploy(writer, dpe, poolsize);
    }


    void deployReconstructionChain(DpeInfo dpe, int poolsize) {
        for (ServiceInfo service : reconstructionChain) {
            deploy(service, dpe, poolsize);
        }
    }


    private void registerContainer(ServiceInfo service, DpeInfo dpe) {
        ContainerName container = new ContainerName(dpe.name, service.cont);
        userContainers.add(container);
    }


    void registerInputOutputContainer(DpeInfo dpe) {
        registerContainer(reader, dpe);
        registerContainer(writer, dpe);
    }


    void registerReconstructionContainers(DpeInfo dpe) {
        for (ServiceInfo service : reconstructionChain) {
            registerContainer(service, dpe);
        }
    }


    ServiceName getStageServiceName(DpeInfo ioDpe) {
        return getServiceName(ioDpe, stage);
    }


    ServiceName getReaderServiceName(DpeInfo ioDpe) {
        return getServiceName(ioDpe, reader);
    }


    ServiceName getWriterServiceName(DpeInfo ioDpe) {
        return getServiceName(ioDpe, writer);
    }


    List<ServiceName> generateReconstructionChain(DpeInfo recDpe) {
        List<ServiceName> servicesNames = new ArrayList<>();
        for (ServiceInfo service : reconstructionChain) {
            ServiceName name = getServiceName(recDpe, service);
            servicesNames.add(name);
        }
        return servicesNames;
    }


    Set<ContainerName> getRegisteredContainers(DpeInfo dpe) {
        try {
            String topic = CConstants.CONTAINER + ":" + dpe.name;
            Set<xMsgRegistration> regData = findSubscribers(feHost, topic);
            Set<ContainerName> regContainers = new HashSet<>();
            for (xMsgRegistration x : regData) {
                regContainers.add(new ContainerName(x.getName()));
            }
            return regContainers;
        } catch (xMsgException e) {
            throw new OrchestratorError(e);
        }
    }


    Set<ServiceName> getRegisteredServices(DpeInfo dpe) {
        try {
            Set<xMsgRegistration> regData = findSubscribers(feHost, dpe.name.canonicalName());
            Set<ServiceName> regServices = new HashSet<>();
            for (xMsgRegistration x : regData) {
                regServices.add(new ServiceName(x.getName()));
            }
            return regServices;
        } catch (xMsgException e) {
            throw new OrchestratorError(e);
        }
    }

    private Set<ServiceName> findMissingServices(List<ServiceName> services,
                                                 Set<ServiceName> regServices) {
        Set<ServiceName> missingServices = new HashSet<>();
        for (ServiceName s : services) {
            if (!regServices.contains(s)) {
                missingServices.add(s);
            }
        }
        return missingServices;
    }


    private void checkServices(DpeInfo dpe, List<ServiceName> services) {
        final int sleepTime = 2000;
        final int totalConnectTime = 1000 * 10 * services.size();
        final int maxAttempts = totalConnectTime / sleepTime;
        final int retryAttempts = maxAttempts / 2;

        int counter = 1;
        while (true) {
            Set<ServiceName> regServices = getRegisteredServices(dpe);
            Set<ServiceName> missingServices = findMissingServices(services, regServices);
            if (missingServices.isEmpty()) {
                return;
            } else {
                if (counter == retryAttempts) {
                    reDeploy(regServices, missingServices);
                }
                counter++;
                if (counter > maxAttempts) {
                    throw new OrchestratorError(reportUndeployed(missingServices));
                }
                sleep(2000);
            }
        }
    }


    private void reDeploy(Set<ServiceName> regServices, Set<ServiceName> missingServices) {
        // Remove user containers that were not started
        Set<ContainerName> regContainers = new HashSet<>();
        for (ServiceName service : regServices) {
            regContainers.add(service.container());
        }
        for (ServiceName missing : missingServices) {
            ContainerName cont = missing.container();
            if (!regContainers.contains(cont)) {
                userContainers.remove(cont);
            }
        }
        // Re-deploy missing services
        for (ServiceName missing : missingServices) {
            DeployedService deployInfo = userServices.get(missing);
            Logging.info("Service " + missing + " was not found. Trying to redeploy...");
            deploy(deployInfo.service, deployInfo.dpe, deployInfo.poolsize);
        }
    }


    private String reportUndeployed(Set<ServiceName> missingServices) {
        StringBuilder sb = new StringBuilder();
        sb.append("undeployed service");
        if (missingServices.size() > 1) {
            sb.append("s");
        }
        sb.append(" = '");
        Iterator<ServiceName> iter = missingServices.iterator();
        sb.append(iter.next());
        while (iter.hasNext()) {
            sb.append(", ");
            sb.append(iter.next());
        }
        sb.append("'");
        return sb.toString();
    }


    boolean findInputOutputService(DpeInfo dpe) {
        List<ServiceName> services = Arrays.asList(getServiceName(dpe, reader),
                                                   getServiceName(dpe, writer));
        Set<ServiceName> regServices = getRegisteredServices(dpe);
        return findMissingServices(services, regServices).isEmpty();
    }


    boolean findReconstructionServices(DpeInfo dpe) {
        Set<ServiceName> regServices = getRegisteredServices(dpe);
        return findMissingServices(generateReconstructionChain(dpe), regServices).isEmpty();
    }


    void checkInputOutputServices(DpeInfo dpe) {
        List<ServiceName> services = Arrays.asList(
                getServiceName(dpe, stage),
                getServiceName(dpe, reader),
                getServiceName(dpe, writer));
        checkServices(dpe, services);
    }


    void checkReconstructionServices(DpeInfo dpe) {
        checkServices(dpe, generateReconstructionChain(dpe));
    }


    void removeUserContainers() {
        for (ContainerName container : userContainers) {
            try {
                base.exit(container).run();
            } catch (ClaraException e) {
                e.printStackTrace();
            }
        }
    }


    private ServiceName getServiceName(DpeInfo dpe, ServiceInfo service) {
        return new ServiceName(dpe.name, service.cont, service.name);
    }


    void listenDpes(DpeCallBack callback) {
        try {
            DpeCallbackWrapper dpeCallback = new DpeCallbackWrapper(callback);
            base.listen().aliveDpes().start(dpeCallback);
        } catch (ClaraException e) {
            throw new OrchestratorError("Could not subscribe to services", e);
        }
    }


    void subscribeErrors(ClaraName name, EngineCallback callback) {
        try {
            base.listen(name).status(EngineStatus.ERROR).start(callback);
        } catch (ClaraException e) {
            throw new OrchestratorError("Could not subscribe to services", e);
        }
    }


    void subscribeDone(ServiceName service, EngineCallback callback) {
        try {
            base.listen(service).done().start(callback);
        } catch (ClaraException e) {
            throw new OrchestratorError("Could not subscribe to services", e);
        }
    }


    public void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }



    private class DeployedService {
        final ServiceInfo service;
        final DpeInfo dpe;
        final int poolsize;

        DeployedService(ServiceInfo service, DpeInfo dpe, int poolsize) {
            this.service = service;
            this.dpe = dpe;
            this.poolsize = poolsize;
        }
    }



    interface DpeCallBack {
        void callback(DpeInfo dpe);
    }



    private class DpeCallbackWrapper implements GenericCallback {

        final DpeCallBack callback;


        DpeCallbackWrapper(DpeCallBack callback) {
            this.callback = callback;
        }


        @Override
        public void callback(String data) {
            try {
                StringTokenizer st = new StringTokenizer(data, "?");
                DpeName name = new DpeName(st.nextToken());
                int ncores = Integer.parseInt(st.nextToken());
                String claraHome = st.nextToken();
                DpeInfo dpe = new DpeInfo(name, ncores, claraHome);
                callback.callback(dpe);
            } catch (NoSuchElementException e) {
                e.printStackTrace();
            }
        }
    }


    // TODO: this should be provided by Clara
    private Set<xMsgRegistration> findSubscribers(String host, String topic)
            throws xMsgException {
        Socket connection = context.createSocket(ZMQ.REQ);
        connection.connect("tcp://" + host + ":" + xMsgConstants.REGISTRAR_PORT);
        try {
            xMsgTopic xtopic = xMsgTopic.wrap(topic);
            xMsgRegistration.Builder regb = xMsgRegistration.newBuilder();
            regb.setName(base.getName());
            regb.setHost(host);
            regb.setPort(xMsgConstants.REGISTRAR_PORT);
            regb.setDomain(xtopic.domain());
            regb.setSubject(xtopic.subject());
            regb.setType(xtopic.type());
            regb.setOwnerType(xMsgRegistration.OwnerType.SUBSCRIBER);
            xMsgRegistration data = regb.build();
            return find(connection, data);
        } finally {
            context.destroySocket(connection);
        }
    }


    private Set<xMsgRegistration> find(Socket connection, xMsgRegistration data)
            throws xMsgException {
        byte[] dt = data.toByteArray();

        ZMsg reqMsg = new ZMsg();
        reqMsg.addString(xMsgConstants.FIND_SUBSCRIBER.toString());
        reqMsg.addString(base.getName());
        reqMsg.add(dt);

        try {
            reqMsg.send(connection, true);
        } catch (ZMQException e) {
            throw new xMsgException("error sending the message");
        } finally {
            reqMsg.destroy();
        }

        ZMQ.Poller poller = new ZMQ.Poller(1);
        poller.register(connection, ZMQ.Poller.POLLIN);
        poller.poll(xMsgConstants.FIND_REQUEST_TIMEOUT);

        if (poller.pollin(0)) {
            ZMsg responseMsg = ZMsg.recvMsg(connection);
            try {
                xMsgRegResponse response = new xMsgRegResponse(responseMsg);
                String status = response.status();
                if (!status.equals(xMsgConstants.SUCCESS.toString())) {
                    throw new xMsgException(status);
                }
                return response.data();
            } finally {
                responseMsg.destroy();
            }
        } else {
            throw new xMsgException("xMsg actor discovery timeout");
        }
    }
}
