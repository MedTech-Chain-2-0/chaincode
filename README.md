# MedTech Chain Chaincode

## Project Structure

- **`config/`**: Contains configuration related to code style.

- **`crypto/ (git ignored)`**: Contains all the crypto material required to run Chaincode as a Service. Use the `copy-crypto.sh` script to copy the crypto material generated in the `tools` repository. (This might change in the future to ease the process)

- **`docker/`**: Contains docker entrypoint script for Chaincode as a Service deployment.

- **`libs/`**: Contains external libraries that are not published on an online repository (e.g., medtechchain protos).

- **`scripts/`**: Contains utility scripts. If the scripts need to access a different repository (e.g., `tools` or `protos`), the developer can either specify the absolute path to the repo or make sure that this repository and the one needing to be accessed have the same parent directory. It is recommended to place all repositories in a common parent directory.

## Source code

- **`nl.medtechchain.chaincode.contract`**: Contains the definitions of the smart contracts.

- **`nl.medtechchain.chaincode.encryption`**: Contains the implementation of homomorphic encryption. (Still in progress and requires careful design choices to facilitate modularity and extensibility.)

## Deployment

Before deploying, use the `scripts/copy-protos.sh` script to copy the proto jar.

### Classic

Currently, the only implemented chaincode deployment is by following the default lifecycle (package, install on peers, ...).
The deployment is performed by automation scripts from the `tools` repository.

### Chaincode as a Service (current)

1. **Prepare the FHE sources** (only when FHE code changes)
   ```bash
   cd chaincode/scripts
   ./copy-fhe.sh
   ```
   This drops a tiny `fhe-src/` folder into the repo; the Dockerfile
   compiles the `bfv_calc` binary from it.

2. **Deploy the chaincode definition to Fabric**
   ```bash
   tools/fabric/cc-deploy-external.sh <cc_version> <cc_seq>
   ```
   Copy the printed **CHAINCODE_ID**.

3. **Paste that ID in `docker-compose.yaml`** (three places, env `CHAINCODE_ID`).

4. **Build and start the service containers**
   ```bash
   docker-compose up --build
   ```
---
### Chaincode as a Service (old)

```text
All Docker-related files are used to run the chaincode as an external service,
but the previous infra wasn't wired for it.  Notes kept for reference:

The compose networks/vars must match the peer network.

openssl req -nodes -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem \
  -subj "/C=NL/ST=Zuid-Holland/L=Delft/O=MedTechChain/CN=chaincode.peer0.medtechchain.nl" \
  -addext "subjectAltName=DNS:chaincode.peer0.medtechchain.nl"

awk 'NF {sub(/\r/, ""); printf "%s\\n",$0;}' key.pem