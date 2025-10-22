import asyncio
import logging
import math
import numpy as np
import inspect
from asyncua import Client
from asyncua.ua import ObjectIds
from asyncua.common.xmlexporter import XmlExporter  
import argparse
from collections import Counter
from asyncua import ua, Server
from urdfpy import URDF
from asyncua.ua.uaerrors import UaStatusCodeError
import os, pathlib, hashlib
from typing import Optional


APP_NS_URI = "URDF-Transformer"

# Optional: if multiple devices exist, set the BrowseName here
DEVICE_NAME = None  # e.g., “Franka” or None for “first found”

# UN/CEFACT UnitIds (frequently used SI units)
UNIT_METER_ID = 5066068
UNIT_RADIAN_ID = 5066062
UNIT_KILOGRAM_ID = 4408652

logging.basicConfig(level=logging.INFO)
logging.getLogger("asyncua").setLevel(logging.WARNING)


# ----------------------------- Helper --------------------------------

def _derive_mesh_dirs_from_urdf(urdf_path: str) -> tuple[str, str]:
    """
    Liest alle <mesh filename="..."> aus der URDF und bestimmt die typischen Basispfade
    für Visuals und Collisions relativ zur URDF. Fällt robust zurück.
    """
    urdf_abs = os.path.abspath(urdf_path)
    urdf_dir = os.path.dirname(urdf_abs)
    urdf = URDF.load(urdf_abs)

    def norm_rel(p: str) -> str:
        # package://<pkg>/... remove and normalize
        rel = _strip_package_prefix(p).replace("\\", "/")
        # leading ./ remove
        if rel.startswith("./"):
            rel = rel[2:]
        return rel

    visual_dirs = []
    collision_dirs = []

    for link in urdf.links:
        # Visuals
        for vis in getattr(link, "visuals", []) or []:
            geom = getattr(vis, "geometry", None)
            if not geom:
                continue
            filenames = []
            mesh = getattr(geom, "mesh", None)
            if mesh is not None:
                filenames = to_list(getattr(mesh, "filename", None))
            else:
                filenames = to_list(getattr(geom, "filename", None))
            for fn in filenames:
                if not fn:
                    continue
                rel = norm_rel(str(fn))
                d = os.path.dirname(rel)
                parts = d.split("/") if d else []
                if "visual" in parts:
                    d = "/".join(parts[:parts.index("visual")+1])  # .../visual
                visual_dirs.append(d or ".")

        # Collisions
        for col in getattr(link, "collisions", []) or []:
            geom = getattr(col, "geometry", None)
            if not geom:
                continue
            filenames = []
            mesh = getattr(geom, "mesh", None)
            if mesh is not None:
                filenames = to_list(getattr(mesh, "filename", None))
            else:
                filenames = to_list(getattr(geom, "filename", None))
            for fn in filenames:
                if not fn:
                    continue
                rel = norm_rel(str(fn))
                d = os.path.dirname(rel)
                parts = d.split("/") if d else []
                if "collision" in parts:
                    d = "/".join(parts[:parts.index("collision")+1])  # .../collision
                collision_dirs.append(d or ".")

    def pick_dir(cands: list[str], default_rel: str) -> str:
        if not cands:
            return os.path.join(urdf_dir, "meshes", default_rel)
        rel_dir = Counter(cands).most_common(1)[0][0]  
        abs_dir = os.path.abspath(os.path.join(urdf_dir, rel_dir))
        return abs_dir

    vis_dir = pick_dir(visual_dirs, "visual")
    col_dir = pick_dir(collision_dirs, "collision")

    # If folders do not exist, do not abort – create & continue
    os.makedirs(vis_dir, exist_ok=True)
    os.makedirs(col_dir, exist_ok=True)
    return vis_dir, col_dir

def _guess_mime_from_ext(path: str) -> str:
    ext = pathlib.Path(path).suffix.lower()
    return {
        ".stl":  "model/stl",
        ".dae":  "model/vnd.collada+xml",
        ".obj":  "model/obj",
        ".ply":  "model/ply",
        ".gltf": "model/gltf+json",
        ".glb":  "model/gltf-binary",
        ".3ds":  "model/3ds",
    }.get(ext, "application/octet-stream")

def _sanitize_browsename(name: str) -> str:
    # OPC UA BrowseNames: no slash/backslash/control characters
    safe = "".join(ch if ch.isalnum() or ch in ("_", "-", ".") else "_" for ch in name)
    return safe[:64] if len(safe) > 64 else safe

def _strip_package_prefix(uri: str) -> str:
    # z.B. „package://fr3_description/meshes/visual/link.dae“ -> „meshes/visual/link.dae“
    if uri.startswith("package://"):
        parts = uri[len("package://"):]
        
        i = parts.find("/")
        return parts[i+1:] if i >= 0 else ""
    return uri

def _try_candidates(base_dir: str, rel_or_name: str) -> Optional[str]:
    """
   Attempts to find a mesh file in base_dir.
    - If rel_or_name contains subfolders, combine directly
    - Otherwise, first search for basename in tree
    """
    
    p = pathlib.Path(base_dir) / rel_or_name
    if p.exists():
        return str(p)

    
    bn = pathlib.Path(rel_or_name).name
    for root, _, files in os.walk(base_dir):
        if bn in files:
            return str(pathlib.Path(root) / bn)
    return None

def resolve_mesh_path(mesh_uri: str, base_dir: str) -> Optional[str]:
    """
    URDF mesh URI -> local path in the specified base_dir.
    ‘package://’ and relative entries.
    """
    if not mesh_uri:
        return None
    rel = _strip_package_prefix(mesh_uri)
    
    return _try_candidates(base_dir, rel)

async def _call_method_by_name(obj, method_name, *args):
    for ch in await obj.get_children():
        if await ch.read_node_class() == ua.NodeClass.Method:
            bn = await ch.read_browse_name()
            if bn.Name == method_name:
                return await obj.call_method(ch.nodeid, *args)
    raise RuntimeError(f"Method {method_name} not found on {obj}")

async def upload_file_to_filetype(file_node, local_path: str, chunk_size=64*1024):
    """
    Writes a local file to an existing FileType node via Open/Write/Close.
We assume that the node implements the standard methods
(e.g., if it was created using add_file).
    """
    mode = ua.Variant(2 | 4, ua.VariantType.UInt32)  # Write | EraseExisting
    handle = (await _call_method_by_name(file_node, "Open", mode))[0]
    try:
        with open(local_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                data = ua.Variant(chunk, ua.VariantType.ByteString)
                await _call_method_by_name(file_node, "Write",
                                           ua.Variant(handle, ua.VariantType.UInt32),
                                           data)
    finally:
        await _call_method_by_name(file_node, "Close",
                                   ua.Variant(handle, ua.VariantType.UInt32))

async def ensure_filetype_with_content(server: Server, parent, idx_ns: int,
                                       name: str, local_path: str,
                                       mime: Optional[str] = None):
    """
   Creates a FileType object (child) under ‘parent’ – with server support if possible – 
    and loads the file chunked into it. Fallback: creates object + ByteString ‘Content’.
    """
    # 0) Prepare MIME + size + hash
    mime = mime or _guess_mime_from_ext(local_path)
    size = os.path.getsize(local_path)
    sha256 = None
    with open(local_path, "rb") as f:
        sha256 = hashlib.sha256(f.read()).hexdigest()

    # 1) Try: parent.add_file(...) (if asyncua offers this)
    file_node = None
    try:
        add_file = getattr(parent, "add_file", None)
        if add_file is not None and callable(add_file):
            # Create empty initially; then fill with Write so that Open/Write/Close paths are used
            file_node = await parent.add_file(idx_ns, name, b"")
    except Exception:
        file_node = None

    # 2) Fallback: explicitly instantiate FileType-ObjectType; if necessary, BaseObject
    if file_node is None:
        try:
            filetype_ot = await resolve_objecttype(server, "FileType", 0)
        except Exception:
            filetype_ot = None
        try:
            if filetype_ot is not None:
                file_node = await parent.add_object(idx_ns, name, objecttype=filetype_ot.nodeid)
            else:
                file_node = await parent.add_object(idx_ns, name)
        except Exception:
            # letzte Rettung
            file_node = await parent.add_object(idx_ns, name)

    # 3) Set metadata
    await upsert_prop_if_missing(file_node, idx_ns, "MimeType", mime, ua.VariantType.String)
    await upsert_prop_if_missing(file_node, idx_ns, "Size", size, ua.VariantType.UInt64)
    await upsert_prop_if_missing(file_node, idx_ns, "sha256", sha256, ua.VariantType.String)
    await upsert_prop_if_missing(file_node, idx_ns, "LocalPath", local_path, ua.VariantType.String)

    # 4) Write content (preferably via Open/Write/Close)
    try:
        await upload_file_to_filetype(file_node, local_path)
    except Exception:
        # Fallback: ByteString property ‘Content’ (not standard-compliant, but usable)
        try:
            with open(local_path, "rb") as f:
                data = f.read()
            await upsert_prop_if_missing(file_node, idx_ns, "Content", data, ua.VariantType.ByteString)
        except Exception:
            pass

    return file_node

async def attach_mesh_files_to_meshnode(server: Server, mesh_node, idx_ns: int,
                                        filenames: list[str], base_dir: str):
    """
   Adds a ‘Files’ folder below ‘mesh_node’. For each filename (URDF),
    if found, a FileType child is created and the file is loaded.
    """
    if not filenames:
        return
    files_folder = await get_or_create_object(mesh_node, idx_ns, "Files")
    for i, fn in enumerate(filenames, start=1):
        local = resolve_mesh_path(fn, base_dir)
        if not local:
            miss = await get_or_create_object(files_folder, idx_ns, _sanitize_browsename(f"Missing_{i}"))
            await upsert_prop_if_missing(miss, idx_ns, "source", str(fn), ua.VariantType.String)
            await upsert_prop_if_missing(miss, idx_ns, "reason", "not found under base_dir", ua.VariantType.String)
            continue
        node_name = _sanitize_browsename(f"{pathlib.Path(local).name}")
        existing = await find_child_by_browsename(files_folder, node_name)
        if existing is None:
            await ensure_filetype_with_content(server, files_folder, idx_ns, node_name, local, _guess_mime_from_ext(local))


async def find_motion_profile_node(axis_node):
    """
    Searches for ‘MotionProfile’ first directly on the Axis, then in the ‘ParameterSet’.
    Returns the node or None.
    """
    mp = await get_child_variable_by_name(axis_node, "MotionProfile")
    if mp is not None:
        return mp
    ps = await find_child_by_browsename(axis_node, "ParameterSet")
    if ps is not None:
        mp = await get_child_variable_by_name(ps, "MotionProfile")
        if mp is not None:
            return mp
    return None


async def ensure_motion_profile_property(
    axis_node,
    idx_ns,
    value_int: int | None,
    enum_datatype_nodeid: ua.NodeId | None,
    write_if_exists: bool,
):
    """
    - Searches for MotionProfile (Axis or ParameterSet).
    
    - If present: 
    - write_if_exists=False  -> never write (existing axis)
    - write_if_exists=True   -> set value (new end axis)
    - If not present:
    - Create property on axis and set initial value (if value_int != None).
    """
    # 1) Search for existing (Axis first, then ParameterSet)
    mp = await get_child_variable_by_name(axis_node, "MotionProfile")
    if mp is None:
        ps = await find_child_by_browsename(axis_node, "ParameterSet")
        if ps is not None:
            mp = await get_child_variable_by_name(ps, "MotionProfile")

    # 2) exists → write if necessary
    if mp is not None:
        if write_if_exists and (value_int is not None):
            try:
                await mp.write_value(value_int)
            except Exception:
                pass
        return mp

    # 3) does NOT exist → create new **at Axis** (only if we have a meaningful value)
    if value_int is None:
        return None

    prop = await axis_node.add_property(
        idx_ns, "MotionProfile", value_int, ua.VariantType.Int32
    )
    await prop.set_writable(False)
    if enum_datatype_nodeid is not None:
        try:
            await prop.set_data_type(enum_datatype_nodeid)
        except Exception:
            pass
    return prop


async def bfs_find_by_browsename(
    server: Server,
    start_node,
    target_name: str,
    ns_index: int | None = None,
    nodeclass: ua.NodeClass | None = None,
):
    seen = set()
    queue = [start_node]
    while queue:
        cur = queue.pop(0)
        if cur.nodeid in seen:
            continue
        seen.add(cur.nodeid)
        try:
            qn = await cur.read_browse_name()
            if (
                qn
                and qn.Name == target_name
                and (ns_index is None or qn.NamespaceIndex == ns_index)
            ):
                if (
                    nodeclass is None
                    or await cur.read_node_class() == nodeclass
                ):
                    return cur
        except Exception:
            pass
        try:
            queue.extend(await cur.get_children())
        except Exception:
            pass
    return None


async def resolve_objecttype(server: Server, name: str, ns_index: int | None):
    return await bfs_find_by_browsename(
        server, server.nodes.types, name, ns_index, ua.NodeClass.ObjectType
    )


async def resolve_datatype(server: Server, name: str, ns_index: int | None):
    return await bfs_find_by_browsename(
        server, server.nodes.types, name, ns_index, ua.NodeClass.DataType
    )


def mp_from_urdf(urdf_joint_type: str):
    """
    AxisMotionProfileEnumeration:
    OTHER=0, ROTARY=1, ROTARY_ENDLESS=2, LINEAR=3, LINEAR_ENDLESS=4
    """
    t = (urdf_joint_type or "").lower()
    if t == "revolute":
        return 1
    if t == "continuous":
        return 2
    if t == "prismatic":
        return 3
    # fixed / floating / planar → no axis; if so: OTHER
    return 0


async def safe_add_axis_object(
    parent, idx_ns: int, browsename: str, axis_type_node
):
    """
    Instantiates AxisType **without optional children** (instantiate_optional=False).
    Fallback: BaseObject if AxisType is not available.
    """
    if axis_type_node is not None:
        try:
            return await parent.add_object(
                idx_ns,
                browsename,
                objecttype=axis_type_node.nodeid,
                instantiate_optional=False,  # <<< only compulsory children
            )
        except UaStatusCodeError:
            pass
        except Exception:
            pass
    return await parent.add_object(idx_ns, browsename)


async def get_namespace_array(server: Server):
    try:
        return await server.get_namespace_array()
    except Exception:
        return server.iserver.namespace_array


async def get_namespace_index_by_hint(
    server: Server, hints=("Robotics", "Robots", "OPC UA Robotics")
) -> int | None:
    ns_arr = await get_namespace_array(server)
    for idx, uri in enumerate(ns_arr):
        if any(h.lower() in uri.lower() for h in hints):
            return idx
    return None


async def upsert_enum_var_if_missing(
    parent,
    idx_ns,
    name,
    value_int: int,
    enum_datatype_nodeid: ua.NodeId | None,
):
    """
    Creates an enum variable (Int32) if it does not exist.
    Sets the DataType to the enum (AxisMotionProfileEnumeration) if available.
    NEVER overwrites existing values.
    """
    var = await get_child_variable_by_name(parent, name)
    if var is None:
        var = await parent.add_variable(
            idx_ns, name, value_int, ua.VariantType.Int32
        )
        await var.set_writable(False)
        if enum_datatype_nodeid is not None:
            try:
                await var.set_data_type(enum_datatype_nodeid)
            except Exception:
                pass  # if Nodeset has a different name, just leave Int32
    return var


async def upsert_prop_if_missing(
    parent, idx_ns, name, value, varianttype=ua.VariantType.Double
):
    """
   Creates a **property** (not a normal variable node) if it does not exist.
    NEVER overwrites existing ones.
    """
    var = await get_child_variable_by_name(parent, name)
    if var is None:
        var = await parent.add_property(
            idx_ns, name, value, varianttype=varianttype
        )
        await var.set_writable(False)
    return var


async def upsert_eu_prop_if_missing(parent, idx_ns, name, value,
                                    unit_symbol, unit_name, unit_id,
                                    varianttype=ua.VariantType.Double):
    prop = await get_child_variable_by_name(parent, name)
    if prop is None:
        prop = await add_eu_prop(parent, idx_ns, name, value,
                                 unit_symbol, unit_name, unit_id, varianttype)
    return prop


async def upsert_enum_prop_if_missing(
    parent,
    idx_ns,
    name,
    value_int: int,
    enum_datatype_nodeid: ua.NodeId | None,
):
    """
    Creates a **property** (Int32), sets DataType to Enum if available.
    NEVER overwrites existing ones.
    """
    prop = await get_child_variable_by_name(parent, name)
    if prop is None:
        prop = await parent.add_property(
            idx_ns, name, value_int, ua.VariantType.Int32
        )
        await prop.set_writable(False)
        if enum_datatype_nodeid is not None:
            try:
                await prop.set_data_type(enum_datatype_nodeid)
            except Exception:
                pass
    return prop


async def add_eu_prop(parent, idx_ns, name, value,
                      unit_symbol, unit_name, unit_id,
                      varianttype=ua.VariantType.Double):
    prop = await parent.add_property(idx_ns, name, value, varianttype=varianttype)
    await prop.set_writable(False)

    eu = ua.EUInformation()
    eu.NamespaceUri = "http://www.opcfoundation.org/UA/units/un/cefact"
    eu.UnitId = unit_id
    eu.DisplayName = ua.LocalizedText(unit_symbol)   
    eu.Description = ua.LocalizedText(unit_name)     

    await prop.add_property(0, "EngineeringUnits",
                            ua.Variant(eu, ua.VariantType.ExtensionObject))
    return prop


async def get_or_create_object(parent, idx_ns, name):
    node = await find_child_by_browsename(parent, name)
    if node:
        return node
    return await parent.add_object(idx_ns, name)


async def get_or_create_folder(parent, idx_ns, name):
    node = await find_child_by_browsename(parent, name)
    if node:
        return node
    return await parent.add_folder(idx_ns, name)


async def get_child_variable_by_name(parent, name: str):
    for child in await parent.get_children():
        if await child.read_node_class() == ua.NodeClass.Variable:
            qn = await child.read_browse_name()
            if qn.Name == name:
                return child
    return None


async def upsert_var_if_missing(
    parent, idx_ns, name, value, varianttype=ua.VariantType.Double
):
    """
    Create a variable if it does NOT exist. Do NOT set a value if it already exists.
    """
    var = await get_child_variable_by_name(parent, name)
    if var is None:
        var = await parent.add_variable(
            idx_ns, name, value, varianttype=varianttype
        )
        await var.set_writable(False)
    return var


async def upsert_eu_var_if_missing(parent, idx_ns, name, value,
                                   unit_symbol, unit_name, unit_id,
                                   varianttype=ua.VariantType.Double):
    var = await get_child_variable_by_name(parent, name)
    if var is None:
        var = await add_eu_var(parent, idx_ns, name, value,
                               unit_symbol, unit_name, unit_id, varianttype)
    return var


def is_linear_joint(jt: str) -> bool:
    return jt == "prismatic"


async def add_eu_var(parent, idx_ns, name, value,
                     unit_symbol, unit_name, unit_id,
                     varianttype=ua.VariantType.Double):
    var = await parent.add_variable(idx_ns, name, value, varianttype=varianttype)
    await var.set_writable(False)

    eu = ua.EUInformation()
    eu.NamespaceUri = "http://www.opcfoundation.org/UA/units/un/cefact"
    eu.UnitId = unit_id
    eu.DisplayName = ua.LocalizedText(unit_symbol)   
    eu.Description = ua.LocalizedText(unit_name)    

    await var.add_property(0, "EngineeringUnits",
                           ua.Variant(eu, ua.VariantType.ExtensionObject))
    return var


def rpy_from_T(T: np.ndarray):
    """Roll, pitch, yaw (rad) from 4x4 homogeneous matrix, convention ZYX."""
    sy = math.sqrt(T[0, 0] ** 2 + T[1, 0] ** 2)
    if sy < 1e-12:
        roll = math.atan2(-T[1, 2], T[1, 1])
        pitch = math.atan2(-T[2, 0], sy)
        yaw = 0.0
    else:
        roll = math.atan2(T[2, 1], T[2, 2])
        pitch = math.atan2(-T[2, 0], sy)
        yaw = math.atan2(T[1, 0], T[0, 0])
    return float(roll), float(pitch), float(yaw)


def to_list(obj):
    if obj is None:
        return []
    if isinstance(obj, (list, tuple)):
        return list(obj)
    return [obj]


async def find_child_by_browsename(parent_node, name: str):
    """Direct child with matching BrowseName.Name (NS irrelevant)."""
    for child in await parent_node.get_children():
        qn = await child.read_browse_name()
        if qn.Name == name:
            return child
    return None


async def find_motion_device_from_system(mds_node, preferred_name: str | None):
    """
 Expects a MotionDeviceSystem node, searches within it:
    MotionDevices -> (optional: preferred_name) -> first MotionDevice object.
    """
    mdevs_folder = await find_child_by_browsename(mds_node, "MotionDevices")
    if not mdevs_folder:
        return None

    devices = await mdevs_folder.get_children()
    # Optional per Name wählen
    if preferred_name:
        for d in devices:
            qn = await d.read_browse_name()
            if qn.Name == preferred_name:
                return d

    # Sonst erstes Objekt nehmen
    for d in devices:
        if await d.read_node_class() == ua.NodeClass.Object:
            return d
    return None


async def resolve_motion_device_node(
    server: Server, inst_nodes, preferred_name: str | None
):
    """
    Robust:
    - If an imported node is already a MotionDevice, use it.
    - If it is a MotionDeviceSystem, select the MotionDevice within it.
    - If unclear, search the Objects hierarchy for ‘MotionDeviceSystem’/'MotionDevices'.
    """
    # 1) Review direct candidates
    for nid in inst_nodes:
        node = server.get_node(nid)
        try:
            mdev = await find_motion_device_from_system(node, preferred_name)
            if mdev:
                return mdev
        except Exception:
            pass

        try:
            if await find_child_by_browsename(node, "Axes"):
                return node
        except Exception:
            pass

    # 2) Broad search from objects
    queue = [server.nodes.objects]
    seen = set()
    while queue:
        cur = queue.pop(0)
        if cur.nodeid in seen:
            continue
        seen.add(cur.nodeid)
        try:
            mdev = await find_motion_device_from_system(cur, preferred_name)
            if mdev:
                return mdev
        except Exception:
            pass
        try:
            queue.extend(await cur.get_children())
        except Exception:
            pass

    # 3) Fallback: first imported node
    return server.get_node(inst_nodes[0])


# ----------------------------- URDF → AddressSpace --------------------------------
async def build_urdf_structure(server: Server, device_node, urdf_path: str,VISUAL_MESH_DIR: str,COLLISION_MESH_DIR: str):
    """
    Create URDF structure below the MotionDevice node:
    - URDF/{Links,Joints,Transmissions,Frames}
    - Add Axes (if not already present)
    """
    idx_app = await server.register_namespace(APP_NS_URI)
    # 1) Load URDF
    urdf = URDF.load(urdf_path)
    # Save the name of the URDF robot as a property (namespace: APP_NS_URI)
    name_prop = await device_node.add_property(
        idx_app, "URDFName", urdf.name, ua.VariantType.String
    )
    await name_prop.set_writable(False)
    



    # 2) Links
    links_folder = await device_node.add_folder(idx_app, "Links")

    for link in urdf.links:
        link_node = await links_folder.add_object(idx_app, link.name)

        # --- Inertial (optional)
        if link.inertial is not None:
            inertial_node = await get_or_create_object(
                link_node, idx_app, "Inertial"
            )

            # Inertial/Origin (xyz, rpy)
            origin_node = await get_or_create_object(
                inertial_node, idx_app, "Origin"
            )
            T_in = (
                link.inertial.origin
                if link.inertial.origin is not None
                else np.eye(4)
            )
            r_in, p_in, y_in = rpy_from_T(T_in)
            await upsert_prop_if_missing(
                origin_node,
                idx_app,
                "xyz",
                [float(T_in[0, 3]), float(T_in[1, 3]), float(T_in[2, 3])],
                ua.VariantType.Double,
            )
            await upsert_prop_if_missing(
                origin_node,
                idx_app,
                "rpy",
                [r_in, p_in, y_in],
                ua.VariantType.Double,
            )

            # Inertial/Mass
            await upsert_eu_var_if_missing(inertial_node, idx_app, "Mass",
                               float(link.inertial.mass),
                               "kg", "kilogram", UNIT_KILOGRAM_ID)


            # Inertial/Inertia (ixx, iyy, izz, ixy, ixz, iyz)
            I = link.inertial.inertia
            inertia_node = await get_or_create_object(
                inertial_node, idx_app, "Inertia"
            )
            await upsert_prop_if_missing(
                inertia_node, idx_app, "ixx", float(I[0, 0])
            )
            await upsert_prop_if_missing(
                inertia_node, idx_app, "iyy", float(I[1, 1])
            )
            await upsert_prop_if_missing(
                inertia_node, idx_app, "izz", float(I[2, 2])
            )
            await upsert_prop_if_missing(
                inertia_node, idx_app, "ixy", float(I[0, 1])
            )
            await upsert_prop_if_missing(
                inertia_node, idx_app, "ixz", float(I[0, 2])
            )
            await upsert_prop_if_missing(
                inertia_node, idx_app, "iyz", float(I[1, 2])
            )

            # --- Visuals (0..n)
        if link.visuals:
            visuals_node = await get_or_create_object(
                link_node, idx_app, "Visuals"
            )
            for idx, vis in enumerate(link.visuals, start=1):
                v_node = await get_or_create_object(
                    visuals_node, idx_app, f"Visual_{idx}"
                )

                vname = getattr(vis, "name", None)
                if vname:
                    await upsert_prop_if_missing(
                        v_node,
                        idx_app,
                        "name",
                        str(vname),
                        ua.VariantType.String,
                    )

                # Visual_i/Origin
                T_v = (
                    vis.origin
                    if getattr(vis, "origin", None) is not None
                    else np.eye(4)
                )
                r_v, p_v, y_v = rpy_from_T(T_v)
                v_origin = await get_or_create_object(
                    v_node, idx_app, "Origin"
                )
                await upsert_prop_if_missing(
                    v_origin,
                    idx_app,
                    "xyz",
                    [float(T_v[0, 3]), float(T_v[1, 3]), float(T_v[2, 3])],
                    ua.VariantType.Double,
                )
                await upsert_prop_if_missing(
                    v_origin,
                    idx_app,
                    "rpy",
                    [r_v, p_v, y_v],
                    ua.VariantType.Double,
                )

                # Visual_i/Geometry (Box|Cylinder|Sphere|Mesh)
                geom = getattr(vis, "geometry", None)
                if geom is not None:
                    g_node = await get_or_create_object(
                        v_node, idx_app, "Geometry"
                    )

                    # --- Mesh ---
                    mesh = getattr(geom, "mesh", None)
                    mesh_filename = None
                    mesh_scale    = None
                    if mesh is not None:
                        mesh_filename = getattr(mesh, "filename", None)
                        mesh_scale    = getattr(mesh, "scale",    None)
                    else:
                        mesh_filename = getattr(geom, "filename", None)
                        mesh_scale    = getattr(geom, "scale",    None)

                    if mesh_filename is not None:
                        mesh_node = await get_or_create_object(g_node, idx_app, "Mesh")
                        filenames = mesh_filename if isinstance(mesh_filename, (list, tuple)) else [mesh_filename]
                        await upsert_prop_if_missing(mesh_node, idx_app, "filename",
                                                    [str(f) for f in filenames], ua.VariantType.String)
                        if mesh_scale is not None:
                            scale_list = np.array(mesh_scale).ravel().tolist()
                            await upsert_prop_if_missing(mesh_node, idx_app, "scale",
                                                        [float(s) for s in scale_list], ua.VariantType.Double)

                        await attach_mesh_files_to_meshnode(server, mesh_node, idx_app, [str(f) for f in filenames], VISUAL_MESH_DIR)

                    # --- Box ---
                    box = getattr(geom, "box", None)
                    box_size = (
                        getattr(box, "size", None)
                        if box is not None
                        else getattr(geom, "size", None)
                    )
                    if box_size is not None:
                        box_node = await get_or_create_object(
                            g_node, idx_app, "Box"
                        )
                        await upsert_eu_prop_if_missing(box_node, idx_app, "size",
                                [float(s) for s in np.array(box_size).ravel().tolist()],
                                "m", "metre", UNIT_METER_ID)

                    # --- Cylinder ---
                    cylinder = getattr(geom, "cylinder", None)
                    cyl_radius = (
                        getattr(cylinder, "radius", None)
                        if cylinder is not None
                        else getattr(geom, "radius", None)
                    )
                    cyl_length = (
                        getattr(cylinder, "length", None)
                        if cylinder is not None
                        else getattr(geom, "length", None)
                    )
                    if (cyl_radius is not None) and (cyl_length is not None):
                        cyl_node = await get_or_create_object(
                            g_node, idx_app, "Cylinder"
                        )
                        await upsert_eu_prop_if_missing(cyl_node, idx_app, "radius",
                                float(cyl_radius), "m", "metre", UNIT_METER_ID)
                        await upsert_eu_prop_if_missing(cyl_node, idx_app, "length",
                                float(cyl_length), "m", "metre", UNIT_METER_ID)

                    # --- Sphere ---
                    sphere = getattr(geom, "sphere", None)
                    sph_radius = (
                        getattr(sphere, "radius", None)
                        if sphere is not None
                        else getattr(geom, "radius", None)
                    )
                    if sph_radius is not None:
                        sph_node = await get_or_create_object(
                            g_node, idx_app, "Sphere"
                        )
                        await upsert_eu_prop_if_missing(sph_node, idx_app, "radius",
                                float(sph_radius), "m", "metre", UNIT_METER_ID)

                # Visual_i/Material (optional; name, color(rgba), texture)
                mat = getattr(vis, "material", None)
                if mat is not None:
                    m_node = await get_or_create_object(
                        v_node, idx_app, "Material"
                    )
                    mname = getattr(mat, "name", None)
                    if mname:
                        await upsert_prop_if_missing(
                            m_node,
                            idx_app,
                            "name",
                            str(mname),
                            ua.VariantType.String,
                        )

                    color = getattr(mat, "color", None)
                    if color is not None:
                        await upsert_prop_if_missing(
                            m_node,
                            idx_app,
                            "rgba",
                            [
                                float(c)
                                for c in np.array(color).ravel().tolist()
                            ],
                            ua.VariantType.Double,
                        )

                    texture = getattr(mat, "texture", None)
                    tex_filename = None
                    if hasattr(texture, "filename"):
                        tex_filename = getattr(texture, "filename", None)
                    elif isinstance(texture, str):
                        tex_filename = texture
                    if tex_filename:
                        await upsert_prop_if_missing(
                            m_node,
                            idx_app,
                            "texture",
                            str(tex_filename),
                            ua.VariantType.String,
                        )

        # --- Collisions (0..n)
        if link.collisions:
            collisions_node = await get_or_create_object(
                link_node, idx_app, "Collisions"
            )
            for idx, col in enumerate(link.collisions, start=1):
                c_node = await get_or_create_object(
                    collisions_node, idx_app, f"Collision_{idx}"
                )

                cname = getattr(col, "name", None)
                if cname:
                    await upsert_prop_if_missing(
                        c_node,
                        idx_app,
                        "name",
                        str(cname),
                        ua.VariantType.String,
                    )

                # Collision_i/Origin
                T_c = (
                    col.origin
                    if getattr(col, "origin", None) is not None
                    else np.eye(4)
                )
                r_c, p_c, y_c = rpy_from_T(T_c)
                c_origin = await get_or_create_object(
                    c_node, idx_app, "Origin"
                )
                await upsert_prop_if_missing(
                    c_origin,
                    idx_app,
                    "xyz",
                    [float(T_c[0, 3]), float(T_c[1, 3]), float(T_c[2, 3])],
                    ua.VariantType.Double,
                )
                await upsert_prop_if_missing(
                    c_origin,
                    idx_app,
                    "rpy",
                    [r_c, p_c, y_c],
                    ua.VariantType.Double,
                )

                # Collision_i/Geometry (Box|Cylinder|Sphere|Mesh)
                geom = getattr(col, "geometry", None)
                if geom is not None:
                    g_node = await get_or_create_object(
                        c_node, idx_app, "Geometry"
                    )

                    # --- Mesh ---
                    mesh = getattr(geom, "mesh", None)
                    mesh_filename = None
                    mesh_scale    = None
                    if mesh is not None:
                        mesh_filename = getattr(mesh, "filename", None)
                        mesh_scale    = getattr(mesh, "scale",    None)
                    else:
                        mesh_filename = getattr(geom, "filename", None)
                        mesh_scale    = getattr(geom, "scale",    None)

                    if mesh_filename is not None:
                        mesh_node = await get_or_create_object(g_node, idx_app, "Mesh")
                        filenames = mesh_filename if isinstance(mesh_filename, (list, tuple)) else [mesh_filename]
                        await upsert_prop_if_missing(mesh_node, idx_app, "filename",
                                                    [str(f) for f in filenames], ua.VariantType.String)
                        if mesh_scale is not None:
                            scale_list = np.array(mesh_scale).ravel().tolist()
                            await upsert_prop_if_missing(mesh_node, idx_app, "scale",
                                                        [float(s) for s in scale_list], ua.VariantType.Double)

                        # FileType-Nodes + Upload
                        await attach_mesh_files_to_meshnode(server, mesh_node, idx_app, [str(f) for f in filenames], COLLISION_MESH_DIR)


                    # --- Box ---
                    box = getattr(geom, "box", None)
                    box_size = (
                        getattr(box, "size", None)
                        if box is not None
                        else getattr(geom, "size", None)
                    )
                    if box_size is not None:
                        box_node = await get_or_create_object(
                            g_node, idx_app, "Box"
                        )
                        await upsert_eu_prop_if_missing(box_node, idx_app, "size",
                                [float(s) for s in np.array(box_size).ravel().tolist()],
                                "m", "metre", UNIT_METER_ID)

                    # --- Cylinder ---
                    cylinder = getattr(geom, "cylinder", None)
                    cyl_radius = (
                        getattr(cylinder, "radius", None)
                        if cylinder is not None
                        else getattr(geom, "radius", None)
                    )
                    cyl_length = (
                        getattr(cylinder, "length", None)
                        if cylinder is not None
                        else getattr(geom, "length", None)
                    )
                    if (cyl_radius is not None) and (cyl_length is not None):
                        cyl_node = await get_or_create_object(
                            g_node, idx_app, "Cylinder"
                        )
                        await upsert_eu_prop_if_missing(cyl_node, idx_app, "radius",
                                float(cyl_radius), "m", "metre", UNIT_METER_ID)
                        await upsert_eu_prop_if_missing(cyl_node, idx_app, "length",
                                float(cyl_length), "m", "metre", UNIT_METER_ID)

                    # --- Sphere ---
                    sphere = getattr(geom, "sphere", None)
                    sph_radius = (
                        getattr(sphere, "radius", None)
                        if sphere is not None
                        else getattr(geom, "radius", None)
                    )
                    if sph_radius is not None:
                        sph_node = await get_or_create_object(
                            g_node, idx_app, "Sphere"
                        )
                        await upsert_eu_prop_if_missing(sph_node, idx_app, "radius",
                                float(sph_radius), "m", "metre", UNIT_METER_ID)

    # 3) Axes under the *MotionDevice* (Companion Spec compliant) --------------------------------
    axes_folder = await find_child_by_browsename(device_node, "Axes")
    if axes_folder is None:
        axes_folder = await device_node.add_folder(idx_app, "Axes")

    # Robotics Typen/Enums
    idx_robotics = await get_namespace_index_by_hint(
        server, ("Robotics", "Robots")
    )
    axis_type_node = await resolve_objecttype(server, "AxisType", idx_robotics)
    mp_enum_node = await resolve_datatype(
        server, "AxisMotionProfileEnumeration", idx_robotics
    )
    mp_enum_nodeid = mp_enum_node.nodeid if mp_enum_node else None

    # --- Determine the kinematic order of the DoF joints from joint_map
    joint_map = dict(urdf.joint_map)  # name -> Joint
    # Establish parent->child joints after Parent-Link
    parent_to_joints = {}
    for j in joint_map.values():
        parent_to_joints.setdefault(j.parent, []).append(j)

    # BFS from base_link via the joint edges
    base_link_name = (
        urdf.base_link.name if hasattr(urdf, "base_link") else "base_link"
    )
    ordered_joints_all = []
    queue = [base_link_name]
    seen_links = set()
    while queue:
        link = queue.pop(0)
        if link in seen_links:
            continue
        seen_links.add(link)
        for j in parent_to_joints.get(link, []):
            ordered_joints_all.append(j)
            queue.append(j.child)

    ordered_dof = [
        j
        for j in ordered_joints_all
        if j.joint_type in ("revolute", "prismatic", "continuous")
    ]
    all_joints = list(joint_map.values())

    # existing axes (skip folder ‘AdditionalJoints’)
    existing_axes = []
    for child in await axes_folder.get_children():
        if await child.read_node_class() != ua.NodeClass.Object:
            continue
        qn = await child.read_browse_name()
        if qn.Name == "AdditionalJoints":
            continue
        if qn.Name.startswith("Axis"):
            existing_axes.append(child)

    # --- Strict axis joint assignment according to kinematic order (no overwriting of MotionProfile!)
    assigned = []
    for i, axis_node in enumerate(existing_axes):
        j = ordered_dof[i] if i < len(ordered_dof) else None
        assigned.append((axis_node, j))

    matched = {j for _, j in assigned if j is not None}

    # --- 3a) Fill existing axes (never overwrite MotionProfile)
    for axis_node, j in assigned:
        if j is None:
            continue

        linear = (j.joint_type == "prismatic")

        _ = await get_or_create_object(axis_node, idx_app, "ParameterSet")

        # MotionProfile: only create if missing
        mp_val = mp_from_urdf(j.joint_type)
        await ensure_motion_profile_property(
            axis_node, idx_app, mp_val, mp_enum_nodeid, write_if_exists=False
        )

        if mp_val == 0:
            await upsert_prop_if_missing(
                axis_node, idx_app, "JointType", str(j.joint_type or "unknown"), ua.VariantType.String
            )

        # Basic data at the axis (these fields are always present in the URDF)
        await upsert_prop_if_missing(axis_node, idx_app, "Name", j.name, ua.VariantType.String)
        await upsert_prop_if_missing(axis_node, idx_app, "ParentLink", j.parent, ua.VariantType.String)
        await upsert_prop_if_missing(axis_node, idx_app, "ChildLink",  j.child,  ua.VariantType.String)

        # Axis direction: only if an axis is specified in the URDF
        if getattr(j, "axis", None) is not None:
            axis_vec = list(j.axis)
            await upsert_prop_if_missing(
                axis_node, idx_app, "Axis_xyz",
                [float(a) for a in axis_vec], ua.VariantType.Double
            )

        # Origin: only if present in URDF
        if getattr(j, "origin", None) is not None:
            T = j.origin
            r, p, y = rpy_from_T(T)
            origin_node = await get_or_create_object(axis_node, idx_app, "Origin")
            await upsert_prop_if_missing(
                origin_node, idx_app, "xyz",
                [float(T[0,3]), float(T[1,3]), float(T[2,3])], ua.VariantType.Double
            )
            await upsert_prop_if_missing(origin_node, idx_app, "rpy", [r, p, y], ua.VariantType.Double)

        # Limit: only if <limit> exists; check each property individually
        lim = getattr(j, "limit", None)
        if lim is not None:
            lower   = getattr(lim, "lower",    None)
            upper   = getattr(lim, "upper",    None)
            vel     = getattr(lim, "velocity", None)
            effort  = getattr(lim, "effort",   None)

            if any(v is not None for v in (lower, upper, vel, effort)):
                limit_node = await get_or_create_object(axis_node, idx_app, "Limit")
                if lower is not None:
                    await upsert_eu_prop_if_missing(limit_node, idx_app, "lower",
                                float(lower),
                                "m" if linear else "rad",
                                "metre" if linear else "radian",
                                UNIT_METER_ID if linear else UNIT_RADIAN_ID)
                if upper is not None:
                    await upsert_eu_prop_if_missing(limit_node, idx_app, "upper",
                                float(upper),
                                "m" if linear else "rad",
                                "metre" if linear else "radian",
                                UNIT_METER_ID if linear else UNIT_RADIAN_ID)
                if vel is not None:
                    await upsert_eu_prop_if_missing(limit_node, idx_app, "velocity",
                                float(vel),
                                "m/s" if linear else "rad/s",
                                "metre per second" if linear else "radian per second",
                                UNIT_METER_ID if linear else UNIT_RADIAN_ID)
                if effort is not None:
                    await upsert_prop_if_missing(limit_node, idx_app, "effort", float(effort), ua.VariantType.Double)

        # Calibration: only if available
        cal = getattr(j, "calibration", None)
        if cal is not None:
            rising  = getattr(cal, "rising",  None)
            falling = getattr(cal, "falling", None)
            if rising is not None or falling is not None:
                cal_node = await get_or_create_object(axis_node, idx_app, "Calibration")
                if rising  is not None: await upsert_prop_if_missing(cal_node, idx_app, "rising",  float(rising))
                if falling is not None: await upsert_prop_if_missing(cal_node, idx_app, "falling", float(falling))

        # Dynamics: only if available
        dyn = getattr(j, "dynamics", None)
        if dyn is not None:
            damping  = getattr(dyn, "damping",  None)
            friction = getattr(dyn, "friction", None)
            if damping is not None or friction is not None:
                dyn_node = await get_or_create_object(axis_node, idx_app, "Dynamics")
                if damping  is not None: await upsert_prop_if_missing(dyn_node, idx_app, "damping",  float(damping))
                if friction is not None: await upsert_prop_if_missing(dyn_node, idx_app, "friction", float(friction))

        # Mimic: only if available
        mimic = getattr(j, "mimic", None)
        if mimic is not None:
            m_joint  = getattr(mimic, "joint",      None)
            m_multi  = getattr(mimic, "multiplier", None)
            m_offset = getattr(mimic, "offset",     None)
            if any(v is not None for v in (m_joint, m_multi, m_offset)):
                mim_node = await get_or_create_object(axis_node, idx_app, "Mimic")
                if m_joint  is not None: await upsert_prop_if_missing(mim_node, idx_app, "joint",      str(m_joint), ua.VariantType.String)
                if m_multi  is not None: await upsert_prop_if_missing(mim_node, idx_app, "multiplier", float(m_multi))
                if m_offset is not None: await upsert_prop_if_missing(mim_node, idx_app, "offset",     float(m_offset))

        # SafetyController: only if available
        sc = getattr(j, "safety_controller", None)
        if sc is not None:
            sll = getattr(sc, "soft_lower_limit", None)
            sul = getattr(sc, "soft_upper_limit", None)
            kv  = getattr(sc, "k_velocity",       None)
            kp  = getattr(sc, "k_position",       None)
            if any(v is not None for v in (sll, sul, kv, kp)):
                sc_node = await get_or_create_object(axis_node, idx_app, "SafetyController")
                if sll is not None:
                    await upsert_eu_prop_if_missing(sc_node, idx_app, "soft_lower_limit",
                                float(sll),
                                "m" if linear else "rad",
                                "metre" if linear else "radian",
                                UNIT_METER_ID if linear else UNIT_RADIAN_ID)
                if sul is not None:
                    await upsert_eu_prop_if_missing(sc_node, idx_app, "soft_upper_limit",
                                float(sll),
                                "m" if linear else "rad",
                                "metre" if linear else "radian",
                                UNIT_METER_ID if linear else UNIT_RADIAN_ID)
                if kv is not None:
                    await upsert_prop_if_missing(sc_node, idx_app, "k_velocity", float(kv))
                if kp is not None:
                    await upsert_prop_if_missing(sc_node, idx_app, "k_position", float(kp))


    # --- 3b) AdditionalJoints: all URDF joints that have NOT been assigned to existing axes
    extra_joints = [j for j in all_joints if j not in matched]
    if extra_joints:
        add_folder = await get_or_create_folder(axes_folder, idx_app, "AdditionalJoints")
        for k, j in enumerate(extra_joints, start=1):
            axis_obj  = await safe_add_axis_object(add_folder, idx_app, f"Axis_{k}", axis_type_node)
            _ = await get_or_create_object(axis_obj, idx_app, "ParameterSet")

            linear = (j.joint_type == "prismatic")
            mp_val = mp_from_urdf(j.joint_type)
            await ensure_motion_profile_property(axis_obj, idx_app, mp_val, mp_enum_nodeid, write_if_exists=True)

            if mp_val == 0:
                await upsert_prop_if_missing(
                    axis_obj, idx_app, "JointType", str(j.joint_type or "unknown"), ua.VariantType.String
                )

            # Basic data (Name/Parent/Child always exist)
            await upsert_prop_if_missing(axis_obj, idx_app, "Name", j.name, ua.VariantType.String)
            await upsert_prop_if_missing(axis_obj, idx_app, "ParentLink", j.parent, ua.VariantType.String)
            await upsert_prop_if_missing(axis_obj, idx_app, "ChildLink",  j.child,  ua.VariantType.String)

            # Axis only if available
            if getattr(j, "axis", None) is not None:
                axis_vec = list(j.axis)
                await upsert_prop_if_missing(
                    axis_obj, idx_app, "Axis_xyz",
                    [float(a) for a in axis_vec], ua.VariantType.Double
                )

            # Origin only if available
            if getattr(j, "origin", None) is not None:
                T = j.origin
                r, p, y = rpy_from_T(T)
                origin_node = await get_or_create_object(axis_obj, idx_app, "Origin")
                await upsert_prop_if_missing(
                    origin_node, idx_app, "xyz",
                    [float(T[0,3]), float(T[1,3]), float(T[2,3])], ua.VariantType.Double
                )
                await upsert_prop_if_missing(origin_node, idx_app, "rpy", [r, p, y], ua.VariantType.Double)

            # Limit only if available
            lim = getattr(j, "limit", None)
            if lim is not None:
                lower   = getattr(lim, "lower",    None)
                upper   = getattr(lim, "upper",    None)
                vel     = getattr(lim, "velocity", None)
                effort  = getattr(lim, "effort",   None)
                if any(v is not None for v in (lower, upper, vel, effort)):
                    limit_node = await get_or_create_object(axis_obj, idx_app, "Limit")
                    if lower is not None:
                        await upsert_eu_prop_if_missing(limit_node, idx_app, "lower",
                                float(lower),
                                "m" if linear else "rad",
                                "metre" if linear else "radian",
                                UNIT_METER_ID if linear else UNIT_RADIAN_ID)
                    if upper is not None:
                        await upsert_eu_prop_if_missing(limit_node, idx_app, "upper",
                                float(upper),
                                "m" if linear else "rad",
                                "metre" if linear else "radian",
                                UNIT_METER_ID if linear else UNIT_RADIAN_ID)
                    if vel is not None:
                        await upsert_eu_prop_if_missing(limit_node, idx_app, "velocity",
                                float(vel),
                                "m/s" if linear else "rad/s",
                                "metre per second" if linear else "radian per second",
                                UNIT_METER_ID if linear else UNIT_RADIAN_ID)
                    if effort is not None:
                        await upsert_prop_if_missing(limit_node, idx_app, "effort", float(effort), ua.VariantType.Double)

            # Calibration only if available
            cal = getattr(j, "calibration", None)
            if cal is not None:
                rising  = getattr(cal, "rising",  None)
                falling = getattr(cal, "falling", None)
                if rising is not None or falling is not None:
                    cal_node = await get_or_create_object(axis_obj, idx_app, "Calibration")
                    if rising  is not None: await upsert_prop_if_missing(cal_node, idx_app, "rising",  float(rising))
                    if falling is not None: await upsert_prop_if_missing(cal_node, idx_app, "falling", float(falling))

            # Dynamics only if available
            dyn = getattr(j, "dynamics", None)
            if dyn is not None:
                damping  = getattr(dyn, "damping",  None)
                friction = getattr(dyn, "friction", None)
                if damping is not None or friction is not None:
                    dyn_node = await get_or_create_object(axis_obj, idx_app, "Dynamics")
                    if damping  is not None: await upsert_prop_if_missing(dyn_node, idx_app, "damping",  float(damping))
                    if friction is not None: await upsert_prop_if_missing(dyn_node, idx_app, "friction", float(friction))

            # Mimic only if available
            mimic = getattr(j, "mimic", None)
            if mimic is not None:
                m_joint  = getattr(mimic, "joint",      None)
                m_multi  = getattr(mimic, "multiplier", None)
                m_offset = getattr(mimic, "offset",     None)
                if any(v is not None for v in (m_joint, m_multi, m_offset)):
                    mim_node = await get_or_create_object(axis_obj, idx_app, "Mimic")
                    if m_joint  is not None: await upsert_prop_if_missing(mim_node, idx_app, "joint",      str(m_joint), ua.VariantType.String)
                    if m_multi  is not None: await upsert_prop_if_missing(mim_node, idx_app, "multiplier", float(m_multi))
                    if m_offset is not None: await upsert_prop_if_missing(mim_node, idx_app, "offset",     float(m_offset))

            # SafetyController only if available
            sc = getattr(j, "safety_controller", None)
            if sc is not None:
                sll = getattr(sc, "soft_lower_limit", None)
                sul = getattr(sc, "soft_upper_limit", None)
                kv  = getattr(sc, "k_velocity",       None)
                kp  = getattr(sc, "k_position",       None)
                if any(v is not None for v in (sll, sul, kv, kp)):
                    sc_node = await get_or_create_object(axis_obj, idx_app, "SafetyController")
                    if sll is not None:
                        await upsert_eu_prop_if_missing(sc_node, idx_app, "soft_lower_limit",
                                float(sll),
                                "m" if linear else "rad",
                                "metre" if linear else "radian",
                                UNIT_METER_ID if linear else UNIT_RADIAN_ID)
                    if sul is not None:
                        await upsert_eu_prop_if_missing(sc_node, idx_app, "soft_upper_limit",
                                float(sll),
                                "m" if linear else "rad",
                                "metre" if linear else "radian",
                                UNIT_METER_ID if linear else UNIT_RADIAN_ID)
                    if kv is not None:
                        await upsert_prop_if_missing(sc_node, idx_app, "k_velocity", float(kv))
                    if kp is not None:
                        await upsert_prop_if_missing(sc_node, idx_app, "k_position", float(kp))

async def _collect_all_nodes(client: Client, allowed_ns: set[int] | None = None):
    """
    BFS across the entire address space (starting from the root folder).
    Collect node objects (not node IDs!). Optionally, export only specific namespaces.
    """
    root = client.get_node(ObjectIds.RootFolder)
    queue = [root]
    seen = set()
    result = []

    while queue:
        node = queue.pop(0)
        nid = node.nodeid
        if nid in seen:
            continue
        seen.add(nid)

        if allowed_ns is None or nid.NamespaceIndex in allowed_ns:
            result.append(node)

        try:
            # Traverse all referenced nodes (forward/backward)
            refs = await node.get_referenced_nodes()
            queue.extend(refs)
        except Exception:
            # Some servers refuse certain links – just skip them
            pass

    return result



async def export_with_opcua_client(endpoint_url: str, out_path: str,
                                   namespace: int | None = None,
                                   username: str | None = None,
                                   password: str | None = None,
                                   include_values: bool = True):
    client = Client(endpoint_url)
    if username:
        client.set_user(username)
        client.set_password(password or "")

    await client.connect()
    try:
        allowed_ns = {namespace} if namespace is not None else None
        nodes = await _collect_all_nodes(client, allowed_ns)

        exporter = XmlExporter(client, export_values=include_values)
        await exporter.build_etree(nodes)
        await exporter.write_xml(out_path)
    finally:
        await client.disconnect()



# ----------------------------- Server-Start --------------------------------
async def main():
    # --- CLI ---
    parser = argparse.ArgumentParser(description="URDF -> OPC UA (und NodeSet-Export)")
    parser.add_argument("urdf_path", help="Path to the URDF file")
    parser.add_argument("robot_xml", help="Path to the robot instance XML (e.g., Franka.xml)")
    args = parser.parse_args()

    URDF_PATH = os.path.abspath(args.urdf_path)
    ROBOT_XML = os.path.abspath(args.robot_xml)

    if not os.path.isfile(URDF_PATH):
        raise FileNotFoundError(f"URDF not found: {URDF_PATH}")
    if not os.path.isfile(ROBOT_XML):
        raise FileNotFoundError(f"Robot instance XML not found: {ROBOT_XML}")

    # Determine mesh folder from URDF
    VISUAL_MESH_DIR, COLLISION_MESH_DIR = _derive_mesh_dirs_from_urdf(URDF_PATH)

    server = Server()
    await server.init()
    server.set_endpoint("opc.tcp://127.0.0.1:4840")
    server.set_server_name("URDF-Transformer")

    # Load type node sets (Foundation/DI/etc. → Robotics/Companion → own)
    for f in ["Opc.Ua.Di.NodeSet2.xml", "Opc.Ua.Robotics.NodeSet2.xml"]:
        await server.import_xml(f)

    # Load instances (dynamically via CLI)
    inst_nodes = await server.import_xml(ROBOT_XML)
    if not inst_nodes:
        raise RuntimeError(f"No instances imported from {ROBOT_XML}.")

    # *** Important: Determine the *MotionDevice* ***
    device_node = await resolve_motion_device_node(server, inst_nodes, DEVICE_NAME)

    # Create URDF structure under MotionDevice (with automatically detected mesh directories)
    await build_urdf_structure(server, device_node, URDF_PATH, VISUAL_MESH_DIR, COLLISION_MESH_DIR)

    # Dynamically derive export file name from instance XML
    base = os.path.splitext(os.path.basename(ROBOT_XML))[0]
    out_file = f"{base}_URDF_enriched.NodeSet2.xml"
    endpoint = "opc.tcp://127.0.0.1:4840"

    async with server:
        await asyncio.sleep(0.3)
        print("Export node set (including values). Please wait...")
        await export_with_opcua_client(endpoint, out_file, include_values=True)
        print(f"Nodeset exports to: {os.path.abspath(out_file)}")
        # Run servers (if desired); keep brief or remove
        while True:
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())


    # Call from terminal:
    # python URDF_2_OPCUA.py "C:\Pfad\zu\deinem\roboter.urdf" "C:\Pfad\zur\Instanz\MeinRoboter.xml"


