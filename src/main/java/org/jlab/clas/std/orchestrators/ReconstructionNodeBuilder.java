package org.jlab.clas.std.orchestrators;

import org.jlab.clara.base.ClaraLang;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

class ReconstructionNodeBuilder {

    private final ApplicationInfo app;
    private final Map<ClaraLang, DpeInfo> dpes;

    private boolean ready = false;

    ReconstructionNodeBuilder(ApplicationInfo application) {
        this.app = application;
        this.dpes = new HashMap<>();

        this.app.getLanguages().forEach(lang -> this.dpes.put(lang, null));
    }

    public void addDpe(DpeInfo dpe) {
        ClaraLang lang = dpe.name.language();
        if (!dpes.containsKey(lang)) {
            Logging.info("Ignoring DPE %s (language not needed)", dpe.name);
            return;
        }
        DpeInfo prev = dpes.put(dpe.name.language(), dpe);
        if (prev != null && !prev.equals(dpe)) {
            Logging.info("Replacing existing DPE %s with %s", prev.name, dpe.name);
        }
        ready = checkReady();
    }

    public boolean isReady() {
        return ready;
    }

    public ReconstructionNode build(ReconstructionOrchestrator orchestrator) {
        return new ReconstructionNode(orchestrator, new ReconstructionApplication(app, dpes));
    }

    private boolean checkReady() {
        for (Entry<ClaraLang, DpeInfo> e : dpes.entrySet()) {
            if (e.getValue() == null) {
                return false;
            }
        }
        return true;
    }
}
