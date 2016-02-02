package org.jlab.clas.std.orchestrators.errors;

public class OrchestratorConfigError extends RuntimeException {

    private static final long serialVersionUID = 7169655555225259425L;

    public OrchestratorConfigError() {
    }

    public OrchestratorConfigError(String message) {
        super(message);
    }

    public OrchestratorConfigError(Throwable cause) {
        super(cause);
    }

    public OrchestratorConfigError(String message, Throwable cause) {
        super(message, cause);
    }

}
