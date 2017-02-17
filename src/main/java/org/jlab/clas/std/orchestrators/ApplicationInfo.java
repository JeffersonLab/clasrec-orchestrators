package org.jlab.clas.std.orchestrators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class ApplicationInfo {

    static final String STAGE = "stage";
    static final String READER = "reader";
    static final String WRITER = "writer";

    private final Map<String, ServiceInfo> ioServices;
    private final List<ServiceInfo> recServices;

    ApplicationInfo(Map<String, ServiceInfo> ioServices, List<ServiceInfo> recServices) {
        this.ioServices = copyServices(ioServices);
        this.recServices = copyServices(recServices);
    }

    private static Map<String, ServiceInfo> copyServices(Map<String, ServiceInfo> ioServices) {
        if (ioServices.get(STAGE) == null) {
            throw new IllegalArgumentException("missing stage service");
        }
        if (ioServices.get(READER) == null) {
            throw new IllegalArgumentException("missing reader service");
        }
        if (ioServices.get(WRITER) == null) {
            throw new IllegalArgumentException("missing writer service");
        }
        return new HashMap<>(ioServices);
    }

    private static List<ServiceInfo> copyServices(List<ServiceInfo> recChain) {
        if (recChain == null) {
            throw new IllegalArgumentException("null reconstruction chain");
        }
        if (recChain.isEmpty()) {
            throw new IllegalArgumentException("empty reconstruction chain");
        }
        return new ArrayList<>(recChain);
    }

    ServiceInfo getStageService() {
        return ioServices.get(STAGE);
    }

    ServiceInfo getReaderService() {
        return ioServices.get(READER);
    }

    ServiceInfo getWriterService() {
        return ioServices.get(WRITER);
    }

    List<ServiceInfo> getIOServices() {
        return Arrays.asList(getStageService(), getReaderService(), getWriterService());
    }

    List<ServiceInfo> getRecServices() {
        return recServices;
    }

    Set<ServiceInfo> getAllServices() {
        return Stream.concat(ioServices.values().stream(), recServices.stream())
                     .collect(Collectors.toSet());
    }
}
