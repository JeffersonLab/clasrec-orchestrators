package org.jlab.clas.std.orchestrators;

/**
 * Stores the general properties of a service.
 * <p>
 * Currently, these properties are:
 * <ul>
 * <li>name (ex: {@code ECReconstruction})
 * <li>the full classpath (ex: {@code org.jlab.clas12.ec.services.ECReconstruction})
 * <li>the container where the service should be deployed (ex: {@code ec-cont})
 * </ul>
 * Note that this class doesn't represent a deployed service in a DPE, but a
 * template that keeps the name and container of the service. Orchestrators should
 * use the data of this class combined with the values in {@link DpeInfo} to
 * fully identify individual deployed services (i.e. the canonical name).
 */
class ServiceInfo {

    final String name;
    final String classpath;
    final String cont;


    ServiceInfo(String classpath, String cont, String name) {
        if (classpath == null) {
            throw new IllegalArgumentException("Null service classpath name");
        }
        if (cont == null) {
            throw new IllegalArgumentException("Null container name");
        }
        if (name == null) {
            throw new IllegalArgumentException("Null service name");
        }
        this.classpath = classpath;
        this.cont = cont;
        this.name = name;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + cont.hashCode();
        result = prime * result + name.hashCode();
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
        if (!(obj instanceof ServiceInfo)) {
            return false;
        }
        ServiceInfo other = (ServiceInfo) obj;
        if (!cont.equals(other.cont)) {
            return false;
        }
        if (!name.equals(other.name)) {
            return false;
        }
        return true;
    }
}
