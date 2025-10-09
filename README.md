# URDF ↔ OPC UA Transformer

Two command-line tools to move a robot model back and forth between **URDF** and an **OPC UA Robotics** address space:

- `URDF_2_OPCUA.py` — **URDF ➜ OPC UA**
- `OPCUA_2_URDF.py` — **OPC UA ➜ URDF**

Both scripts are designed for reproducible, file-based pipelines and work on Windows, Linux, and macOS.

---

## Features

- **URDF ➜ OPC UA**
  - Creates a Robotics Companion-Spec structure under a `MotionDevice`.
  - Imports a robot *instance* XML (e.g., `Franka.xml`) into the server.
  - Uploads mesh assets (STL/DAE/…) as OPC UA **FileType** nodes with metadata (`MimeType`, `Size`, `sha256`, `LocalPath`).
  - Automatically derives **visual** and **collision** mesh directories from the **relative paths inside the URDF**.

- **OPC UA ➜ URDF**
  - Connects to a running OPC UA server, discovers a `MotionDevice`, and reconstructs a URDF.
  - Exports mesh files to a compact directory structure and references them with **relative paths** in the URDF:

    ```
    <robot>_description/
      ├─ <robot>.urdf
      └─ meshes/
         ├─ visuals/
         └─ collisions/
    ```

- **Joint type preservation**
  - Maps `MotionProfile` to URDF joint types:
    - `1 → revolute`, `2 → continuous`, `3 → prismatic`
    - Otherwise uses a fallback string property **`JointType`** to avoid losing information (`fixed` as last resort).

---

## Requirements
- **Python** ≥ 3.10
- **Packages**
  ```bash
  pip install asyncua urdfpy numpy
  ```
---


 ### Robotics Nodeset XMLs

* `Devices.xml` (OPC UA DI)
* `Robots.xml` (OPC UA Robotics)
* A robot **instance** XML (e.g., `Franka.xml`) that defines the concrete `MotionDevice` or `MotionDeviceSystem` you want to enrich with URDF content.

> Place these XML files where the script can read them (same folder as the script is convenient).

---

## Installation

1. Ensure Python ≥ 3.10.
2. Install dependencies:

   ```bash
   pip install asyncua urdfpy numpy
   ```
3. Put the two scripts into your project:

   * `URDF_2_OPCUA.py`
   * `OPCUA_2_URDF.py`
4. Ensure `Devices.xml`, `Robots.xml`, and your instance XML (e.g., `Franka.xml`) are available.

---

## Usage

### 1) URDF ➜ OPC UA

**Script:** `URDF_2_OPCUA.py`
Starts a local OPC UA server, imports the Robotics type and instance XMLs, then builds a URDF-based structure under the detected `MotionDevice`.

**CLI**

```bash
python URDF_2_OPCUA.py "<PATH_TO_URDF_FILE>" "<PATH_TO_ROBOT_INSTANCE_XML>"
```

**Arguments (required)**

* `PATH_TO_URDF_FILE` — e.g., `C:\robots\fr3_description\urdf\fr3.urdf`
* `PATH_TO_ROBOT_INSTANCE_XML` — e.g., `C:\nodesets\Franka.xml`

**Behavior**

* Server listens at `opc.tcp://127.0.0.1:4840` by default.
* **Visual/Collision mesh directories are derived automatically** from the **relative mesh paths inside the URDF** (no extra CLI flags needed).
* Mesh assets (STL/DAE/…) are uploaded as OPC UA **FileType** nodes with metadata (`MimeType`, `Size`, `sha256`, `LocalPath`). If FileType methods are not available, the script falls back to a `Content` (ByteString) property.

**Examples**

```bash
# Windows
python URDF_2_OPCUA.py "C:\robots\fr3_description\urdf\fr3.urdf" "C:\nodesets\Franka.xml"

# Linux / macOS
python URDF_2_OPCUA.py "/home/me/robots/fr3_description/urdf/fr3.urdf" "/home/me/nodesets/Franka.xml"
```

---

### 2) OPC UA ➜ URDF

**Script:** `OPCUA_2_URDF.py`
Connects to a running OPC UA server, discovers a `MotionDevice`, exports mesh files back to disk, and writes a URDF that references them **relatively**.

**CLI**

```bash
python OPCUA_2_URDF.py "<OPC_UA_ENDPOINT>" -o "<OUT_URDF_PATH>"
```

**Arguments**

* `OPC_UA_ENDPOINT` (required) — e.g., `opc.tcp://127.0.0.1:4840`
* `-o, --out` (optional) — Output URDF path or basename (default: `recovered_robot.urdf` in the current directory)

**Output layout**

```
<robot>_description/
  ├─ <robot>.urdf
  └─ meshes/
     ├─ visuals/
     └─ collisions/
```

URDF `<mesh filename="...">` attributes use **relative paths** into these folders.

**Examples**

```bash
python OPCUA_2_URDF.py "opc.tcp://127.0.0.1:4840"
python OPCUA_2_URDF.py "opc.tcp://127.0.0.1:4840" -o "C:\tmp\recovered_robot.urdf"
python OPCUA_2_URDF.py "opc.tcp://127.0.0.1:4840" --out "./export/recovered_robot.urdf"
```

---

## Typical Roundtrip

1. **Load URDF into OPC UA**

   ```bash
   python URDF_2_OPCUA.py "C:\...\fr3.urdf" "C:\...\Franka.xml"
   ```
2. Inspect/validate the address space with your OPC UA client (e.g., UaExpert).
3. **Rebuild URDF from OPC UA**

   ```bash
   python OPCUA_2_URDF.py "opc.tcp://127.0.0.1:4840" -o "C:\tmp\recovered_robot.urdf"
   ```

---

## How It Works (Key Points)

* **Meshes in OPC UA**

  * Each `Mesh` node gets a `Files` folder containing one or more **FileType** objects holding the bytes.
  * Transfer uses standard `Open/Read/Write/Close`. If missing, a fallback `Content` (ByteString) property is used.
  * Metadata captured: `MimeType`, `Size`, `sha256`, `LocalPath`.

* **Meshes back to disk**

  * The exporter prefers FileType methods; otherwise attempts `Content`.
  * If server content is missing, it tries `LocalPath` or the original `filename` when locally accessible.
  * Files are written under `<robot>_description/meshes/{visuals|collisions}/…` and referenced **relatively**.

* **Joint types**

  * `MotionProfile` → URDF:

    * `1 = revolute`, `2 = continuous`, `3 = prismatic`
  * If unknown/`0`, a companion string property **`JointType`** is used to preserve intent; final fallback is `fixed`.

* **Mesh directory derivation**

  * The URDF ➜ OPC UA script scans `<mesh filename="...">` paths and infers the **visual** and **collision** roots from those **relative paths**.

---

## Troubleshooting

* **`BadNodeIdExists` during XML import**

  * Cause: NodeId collisions when importing an instance XML more than once or against a server already containing those NodeIds.
  * Fixes:

    * Start from a fresh server instance.
    * Ensure you aren’t importing the same instance XML multiple times.
    * If you control the XML, consider letting the SDK auto-assign NodeIds or adjust the NodeId policy.

* **Meshes not exported back**

  * Check that the server supports FileType methods or that a `Content` property exists.
  * If neither exists, the exporter attempts `LocalPath` or original `filename` paths—ensure those are reachable from the exporting machine.

* **Wrong or missing units**

  * The scripts record EngineeringUnits where available, but URDF only accepts certain attributes. Verify your downstream toolchains.

* **Can’t find MotionDevice**

  * The OPC UA ➜ URDF script does a BFS under `Objects`:

    * Looks for `MotionDeviceSystem` → `MotionDevices` → first device.
    * As a heuristic, any object with an `Axes` child is considered a device.
  * If your model deviates, you may need to adapt discovery logic.

---

## Known Limitations

* Complex, server-specific browsing rules or access controls may block reading FileType content.
* Some custom Robotics variants/namespaces may require adapting browse names or type resolution.
* Only a subset of URDF elements is round-tripped (focus on links/joints, geometry, and common metadata).

---

## Contributing

Issues and PRs are welcome

---

