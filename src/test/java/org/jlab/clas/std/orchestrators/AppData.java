package org.jlab.clas.std.orchestrators;

import org.jlab.clara.base.ClaraLang;
import org.jlab.clara.base.DpeName;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class AppData {

    static final int CORES = 5;

    static final String CONT1 = "master";
    static final String CONT2 = "slave";

    static final ServiceInfo S1 = service(CONT1, "S1", ClaraLang.JAVA);
    static final ServiceInfo R1 = service(CONT1, "R1", ClaraLang.JAVA);
    static final ServiceInfo W1 = service(CONT1, "W1", ClaraLang.JAVA);

    static final ServiceInfo J1 = service(CONT1, "J1", ClaraLang.JAVA);
    static final ServiceInfo J2 = service(CONT1, "J2", ClaraLang.JAVA);
    static final ServiceInfo J3 = service(CONT1, "J3", ClaraLang.JAVA);

    static final ServiceInfo K1 = service(CONT2, "K1", ClaraLang.JAVA);
    static final ServiceInfo K2 = service(CONT2, "K2", ClaraLang.JAVA);

    static final DpeInfo DPE1 = dpe("10.1.1.10_java");

    private AppData() { }


    static final class AppBuilder {

        ApplicationInfo app;
        DpeInfo dpe;

        private AppBuilder() {
            app = defaultAppInfo();
            dpe = DPE1;
        }

        AppBuilder withServices(ServiceInfo... services) {
            this.app = new ApplicationInfo(ioServices(), recServices(services));
            return this;
        }

        AppBuilder withDpe(DpeInfo dpe) {
            this.dpe = dpe;
            return this;
        }

        ReconstructionApplication build() {
            return new ReconstructionApplication(app, dpe);
        }
    }


    static AppBuilder builder() {
        return new AppBuilder();
    }


    static ApplicationInfo defaultAppInfo() {
        return newAppInfo(J1, J2, J3);
    }


    static ApplicationInfo newAppInfo(ServiceInfo... services) {
        return new ApplicationInfo(ioServices(), recServices(services));
    }


    static Map<String, ServiceInfo> ioServices() {
        Map<String, ServiceInfo> map = new HashMap<>();
        map.put(ApplicationInfo.STAGE, S1);
        map.put(ApplicationInfo.READER, R1);
        map.put(ApplicationInfo.WRITER, W1);
        return map;
    }


    static List<ServiceInfo> recServices(ServiceInfo... elem) {
        return Arrays.asList(elem);
    }


    static ServiceInfo service(String cont, String engine, ClaraLang lang) {
        return new ServiceInfo("org.test." + engine, cont, engine, lang);
    }


    static DpeInfo dpe(String name) {
        return new DpeInfo(new DpeName(name), CORES, "");
    }
}
