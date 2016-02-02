# clasrec-orchestrators

CLARA orchestrators to process CLAS12 files in a chain of reconstruction
services. The chain should be configured with your selected services.

## Local orchestrator

The `LocalOrchestrator` starts an orchestrator that allows you to quickly test
a reconstruction chain in your local box, by processing a local input file
with a fixed number of threads. A local Clara DPE must be running.

The orchestrator will deploy the reconstruction chain in the local DPE.
Once the chain is deployed, it will request the standard IO reader service
to start sending events to the chain, until all events are processed.
The orchestrator will exit after the file has been reconstructed.

The average reconstruction time per event will be printed in the standard
output, and the reconstructed events will be saved in the specified output
file.

<a href="https://asciinema.org/a/35935" target="_blank"><img src="https://asciinema.org/a/35935.png" width="600"/></a>

