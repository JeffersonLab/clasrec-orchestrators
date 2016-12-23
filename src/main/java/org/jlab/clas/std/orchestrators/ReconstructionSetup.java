package org.jlab.clas.std.orchestrators;

import org.jlab.clara.base.DpeName;
import org.jlab.clara.engine.EngineDataType;

import java.util.List;
import java.util.Map;
import java.util.Set;

class ReconstructionSetup {

    final DpeName frontEnd;
    final String session;

    final List<ServiceInfo> recChain;
    final Map<String, ServiceInfo> ioServices;
    final Set<EngineDataType> dataTypes;

    ReconstructionSetup(DpeName frontEnd,
                        Map<String, ServiceInfo> ioServices,
                        List<ServiceInfo> recChain,
                        Set<EngineDataType> dataTypes,
                        String session) {
        this.frontEnd = frontEnd;
        this.session = session;
        this.ioServices = ioServices;
        this.recChain = recChain;
        this.dataTypes = dataTypes;
    }
}
