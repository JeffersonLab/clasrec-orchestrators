package org.jlab.clas.std.orchestrators;

import org.jlab.clara.base.DpeName;

import java.util.List;

class ReconstructionSetup {

    final DpeName frontEnd;
    final String session;
    final List<ServiceInfo> recChain;

    ReconstructionSetup(DpeName frontEnd,
                        List<ServiceInfo> recChain,
                        String session) {
        this.frontEnd = frontEnd;
        this.session = session;
        this.recChain = recChain;
    }
}
