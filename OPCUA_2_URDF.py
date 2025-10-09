import asyncio
import os, pathlib, base64
from typing import Optional, List, Tuple


from asyncua import Client, ua
from asyncua.ua import ObjectIds
import xml.etree.ElementTree as ET

def _prepare_urdf_root(out_urdf_path: str, robot_name: str) -> tuple[str, str, str, str]:
    """
    Legt urdf/, urdf/meshes/, urdf/meshes/visuals, urdf/meshes/collisions an.
    Gibt (urdf_root_dir, out_urdf_path_final, vis_dir, col_dir) zurück.
    """
    base_dir = os.path.dirname(os.path.abspath(out_urdf_path)) or "."
    urdf_root_dir = os.path.join(base_dir, f"{robot_name}_description")
    os.makedirs(urdf_root_dir, exist_ok=True)

    # URDF-Dateiname: falls keine .urdf-Endung übergeben, robot_name verwenden
    if not out_urdf_path.lower().endswith(".urdf"):
        out_urdf_final = os.path.join(urdf_root_dir, f"{robot_name}.urdf")
    else:
        out_urdf_final = os.path.join(urdf_root_dir, os.path.basename(out_urdf_path))

    meshes_dir = os.path.join(urdf_root_dir, "meshes")
    vis_dir    = os.path.join(meshes_dir, "visuals")
    col_dir    = os.path.join(meshes_dir, "collisions")
    os.makedirs(vis_dir, exist_ok=True)
    os.makedirs(col_dir, exist_ok=True)
    return urdf_root_dir, out_urdf_final, vis_dir, col_dir

_MIME_TO_EXT = {
    "model/stl": ".stl",
    "model/vnd.collada+xml": ".dae",
    "model/obj": ".obj",
    "model/ply": ".ply",
    "model/gltf+json": ".gltf",
    "model/gltf-binary": ".glb",
    "model/3ds": ".3ds",
}

def _ext_from_name_or_mime(name: str | None, mime: str | None) -> str:
    if name:
        sfx = pathlib.Path(name).suffix.lower()
        if sfx:
            return sfx
    if mime and mime in _MIME_TO_EXT:
        return _MIME_TO_EXT[mime]
    return ".stl"

# -------- Pfade & Ordner --------
def _ensure_mesh_dirs(out_urdf_path: str):
    base_dir = os.path.dirname(os.path.abspath(out_urdf_path)) or "."
    meshes_dir = os.path.join(base_dir, "meshes")
    vis_dir    = os.path.join(meshes_dir, "visuals")
    col_dir    = os.path.join(meshes_dir, "collisions")
    os.makedirs(vis_dir, exist_ok=True)
    os.makedirs(col_dir, exist_ok=True)
    return meshes_dir, vis_dir, col_dir




# -------- FileType lesen (Open/Read/Close) --------
async def _call_method_by_name(obj, method_name, *args):
    for ch in await obj.get_children():
        if await ch.read_node_class() == ua.NodeClass.Method:
            bn = await ch.read_browse_name()
            if bn.Name == method_name:
                return await obj.call_method(ch.nodeid, *args)
    raise RuntimeError(f"Method {method_name} not found on {obj}")

async def _read_filetype_bytes(file_node, chunk=64*1024) -> bytes | None:
    try:
        try:
            mode = ua.Variant(1, ua.VariantType.UInt32)  # Read
            handle = (await _call_method_by_name(file_node, "Open", mode))[0]
        except TypeError:
            mode = ua.Variant(1, ua.VariantType.Byte)
            handle = (await _call_method_by_name(file_node, "Open", mode))[0]

        buf = bytearray()
        try:
            while True:
                res = await _call_method_by_name(
                    file_node, "Read",
                    ua.Variant(handle, ua.VariantType.UInt32),
                    ua.Variant(int(chunk), ua.VariantType.Int32),
                )
                bs = res[0] if isinstance(res, (list, tuple)) else res
                if not bs: break
                buf.extend(bs)
                if isinstance(res, (list, tuple)) and len(res) > 1:
                    try:
                        if bool(res[1]): break
                    except Exception:
                        pass
            return bytes(buf)
        finally:
            try:
                await _call_method_by_name(
                    file_node, "Close",
                    ua.Variant(handle, ua.VariantType.UInt32)
                )
            except Exception:
                pass
    except Exception:
        pass

    # Fallback: Content-Property
    try:
        for ch in await file_node.get_children():
            if await ch.read_node_class() == ua.NodeClass.Variable:
                bn = await ch.read_browse_name()
                if bn.Name == "Content":
                    v = await ch.read_value()
                    if isinstance(v, (bytes, bytearray)): return bytes(v)
                    if isinstance(v, str):
                        try: return base64.b64decode(v)
                        except Exception: return None
    except Exception:
        pass
    return None

# -------- Mesh-Datei aus Mesh-Node extrahieren & speichern --------
async def _first_filetype_child(mesh_node):
    for ch in await mesh_node.get_children():
        bn = await ch.read_browse_name()
        if bn.Name == "Files":
            for fch in await ch.get_children():
                if await fch.read_node_class() == ua.NodeClass.Object:
                    return fch
    return None

async def _collect_file_meta(file_node):
    lp = None; mt = None
    for ch in await file_node.get_children():
        if await ch.read_node_class() != ua.NodeClass.Variable: continue
        bn = await ch.read_browse_name()
        if bn.Name == "LocalPath":
            try: lp = await ch.read_value()
            except: pass
        elif bn.Name == "MimeType":
            try: mt = await ch.read_value()
            except: pass
    return (pathlib.Path(lp).name if lp else None), mt

async def _export_mesh_asset(mesh_node, target_dir: str, basename_hint: str, urdf_root_dir: str) -> str | None:
    fnode = await _first_filetype_child(mesh_node)

    # Mesh/filename auslesen
    filename_list = None
    for ch in await mesh_node.get_children():
        if await ch.read_node_class() == ua.NodeClass.Variable:
            bn = await ch.read_browse_name()
            if bn.Name == "filename":
                try: filename_list = await ch.read_value()
                except: pass
                break
    fname0 = (str(filename_list[0]) if isinstance(filename_list, (list, tuple)) and filename_list
              else (filename_list if isinstance(filename_list, str) else None))

    name_hint, mime = (None, None)
    if fnode:
        name_hint, mime = await _collect_file_meta(fnode)

    ext = _ext_from_name_or_mime(fname0 or name_hint, mime)
    stem = pathlib.Path(fname0 or name_hint or basename_hint).stem or basename_hint
    out_path = pathlib.Path(target_dir) / f"{stem}{ext}"
    i = 1
    while out_path.exists():
        out_path = pathlib.Path(target_dir) / f"{stem}_{i}{ext}"
        i += 1

    data = await _read_filetype_bytes(fnode) if fnode else None
    if data is None and fname0 and pathlib.Path(fname0).exists():
        data = pathlib.Path(fname0).read_bytes()
    if data is None and name_hint and pathlib.Path(name_hint).exists():
        data = pathlib.Path(name_hint).read_bytes()
    if data is None:
        return None

    out_path.write_bytes(data)
    # relativer Pfad zur URDF (urdf_root_dir als Bezug)
    rel = os.path.relpath(out_path, start=urdf_root_dir).replace("\\", "/")
    return rel


# ----------------- kleine XML-Helpers -----------------
def _fmt_xyz(vals: Optional[List[float]]) -> Optional[str]:
    if not vals: return None
    return " ".join(f"{float(v):.9g}" for v in vals)

def _set_attr_if(el: ET.Element, key: str, val):
    if val is None: return
    if isinstance(val, (list, tuple)):
        val = _fmt_xyz(val)
        if val is None: return
    el.set(key, str(val))

def _add_child(parent: ET.Element, tag: str, **attrs) -> ET.Element:
    ch = ET.SubElement(parent, tag)
    for k,v in attrs.items():
        if v is None: continue
        ch.set(k, str(v))
    return ch

# ----------------- OPC UA Helpers (Client) -----------------
async def get_child_by_name(node, name: str):
    for ch in await node.get_children():
        bn = await ch.read_browse_name()
        if bn.Name == name:
            return ch
    return None

async def get_child_value(node, name: str, default=None):
    ch = await get_child_by_name(node, name)
    if ch is None: return default
    try:
        return await ch.read_value()
    except Exception:
        return default

async def list_children(node):
    children = []
    for ch in await node.get_children():
        bn = await ch.read_browse_name()
        children.append((bn.Name, ch))
    return children

async def find_motion_device(client) -> Optional[object]:
    """Sucht ab Objects nach MotionDeviceSystem → MotionDevices → erstes Device;
       oder ein Objekt mit 'Axes'-Kind als Heuristik."""
    root = client.get_node(ObjectIds.RootFolder)
    objects = await root.get_child(["0:Objects"])

    # BFS
    queue = [objects]
    seen = set()
    while queue:
        cur = queue.pop(0)
        if cur.nodeid in seen:
            continue
        seen.add(cur.nodeid)

        try:
            # MotionDeviceSystem?
            mds = await get_child_by_name(cur, "MotionDevices")
            if mds:
                # nimm erstes Object darunter
                for _, dev in await list_children(mds):
                    # nur echte Objekte
                    if await dev.read_node_class() == ua.NodeClass.Object:
                        return dev
        except Exception:
            pass

        # Heuristik: hat 'Axes'?
        try:
            if await get_child_by_name(cur, "Axes"):
                return cur
        except Exception:
            pass

        try:
            for _, ch in await list_children(cur):
                queue.append(ch)
        except Exception:
            pass
    return None

# ----------------- Mesh-Pfad aus Mesh-Knoten ermitteln -----------------
async def resolve_mesh_filename(mesh_node) -> Optional[str]:
    # 1) bevorzugt: Mesh/filename (Liste) → erstes Element
    fns = await get_child_value(mesh_node, "filename", None)
    if isinstance(fns, (list, tuple)) and fns:
        return str(fns[0])
    if isinstance(fns, str) and fns:
        return fns

    # 2) fallback: Mesh/Files/*/LocalPath → erstes vorhandenes
    files_folder = await get_child_by_name(mesh_node, "Files")
    if files_folder:
        for _, fchild in await list_children(files_folder):
            lp = await get_child_value(fchild, "LocalPath", None)
            if lp:
                return str(lp)
    return None

# ----------------- Visual/Collision-Block lesen -----------------
async def build_visual_or_collision(parent_node, tag_name: str,
                                    vis_dir: str, col_dir: str,
                                    link_name: str, urdf_root_dir: str) -> list[ET.Element]:
    out = []
    items_node = await get_child_by_name(parent_node, tag_name)
    if not items_node:
        return out

    idx_cnt = 0
    for name, viscol in await list_children(items_node):
        if not name.lower().startswith(tag_name[:-1].lower()):  # Visual_ / Collision_
            continue
        idx_cnt += 1
        vc_el = ET.Element(tag_name[:-1])  # 'visual' oder 'collision'

        # Origin
        origin_node = await get_child_by_name(viscol, "Origin")
        if origin_node:
            xyz = await get_child_value(origin_node, "xyz", None)
            rpy = await get_child_value(origin_node, "rpy", None)
            o_el = ET.SubElement(vc_el, "origin")
            if xyz: o_el.set("xyz", " ".join(map(str, xyz)))
            if rpy: o_el.set("rpy", " ".join(map(str, rpy)))

        # Geometry
        geom_node = await get_child_by_name(viscol, "Geometry")
        if geom_node:
            g_el = ET.SubElement(vc_el, "geometry")

            # Mesh
            mesh_node = await get_child_by_name(geom_node, "Mesh")
            if mesh_node:
                subdir = vis_dir if tag_name == "Visuals" else col_dir
                base = f"{link_name}_{tag_name[:-1].lower()}_{idx_cnt}"
                rel = await _export_mesh_asset(mesh_node, subdir, base, urdf_root_dir)
                m_el = ET.SubElement(g_el, "mesh")
                if rel:
                    m_el.set("filename", rel)
                else:
                    fn = await get_child_value(mesh_node, "filename", None)
                    if isinstance(fn, (list, tuple)) and fn:
                        m_el.set("filename", str(fn[0]))
                    elif isinstance(fn, str) and fn:
                        m_el.set("filename", fn)

            # Box / Cylinder / Sphere
            box_node = await get_child_by_name(geom_node, "Box")
            if box_node:
                size = await get_child_value(box_node, "size", None)
                if size: ET.SubElement(g_el, "box").set("size", " ".join(map(str, size)))
            cyl_node = await get_child_by_name(geom_node, "Cylinder")
            if cyl_node:
                radius = await get_child_value(cyl_node, "radius", None)
                length = await get_child_value(cyl_node, "length", None)
                el = ET.SubElement(g_el, "cylinder")
                if radius is not None: el.set("radius", str(radius))
                if length is not None: el.set("length", str(length))
            sph_node = await get_child_by_name(geom_node, "Sphere")
            if sph_node:
                radius = await get_child_value(sph_node, "radius", None)
                if radius is not None:
                    ET.SubElement(g_el, "sphere").set("radius", str(radius))

        # Material (nur für Visuals)
        if tag_name == "Visuals":
            mat_node = await get_child_by_name(viscol, "Material")
            if mat_node:
                m_el = ET.SubElement(vc_el, "material")
                mname = await get_child_value(mat_node, "name", None)
                if mname: m_el.set("name", str(mname))
                rgba = await get_child_value(mat_node, "rgba", None)
                if rgba: ET.SubElement(m_el, "color").set("rgba", " ".join(map(str, rgba)))
                tex = await get_child_value(mat_node, "texture", None)
                if tex: ET.SubElement(m_el, "texture").set("filename", str(tex))

        out.append(vc_el)
    return out

# ----------------- Links lesen -----------------
async def read_links(device_node, vis_dir: str, col_dir: str, urdf_root_dir: str) -> list[ET.Element]:
    links_el = []
    links_folder = await get_child_by_name(device_node, "Links")
    if not links_folder:
        return links_el

    for lname, lnode in await list_children(links_folder):
        if await lnode.read_node_class() != ua.NodeClass.Object:
            continue
        link_el = ET.Element("link", {"name": lname})

        # Inertial (wie gehabt) ...
        inertial_node = await get_child_by_name(lnode, "Inertial")
        if inertial_node:
            inertial_el = ET.SubElement(link_el, "inertial")
            origin_node = await get_child_by_name(inertial_node, "Origin")
            if origin_node:
                xyz = await get_child_value(origin_node, "xyz", None)
                rpy = await get_child_value(origin_node, "rpy", None)
                o_el = ET.SubElement(inertial_el, "origin")
                if xyz: o_el.set("xyz", " ".join(map(str, xyz)))
                if rpy: o_el.set("rpy", " ".join(map(str, rpy)))
            mass = await get_child_value(inertial_node, "Mass", None)
            if mass is not None: ET.SubElement(inertial_el, "mass").set("value", str(mass))
            I = await get_child_by_name(inertial_node, "Inertia")
            if I:
                vals = {k: await get_child_value(I, k, None) for k in ("ixx","iyy","izz","ixy","ixz","iyz")}
                ET.SubElement(inertial_el, "inertia", **{k:str(v) for k,v in vals.items() if v is not None})

        # Visuals / Collisions mit Asset-Export in urdf/meshes/...
        for v_el in await build_visual_or_collision(lnode, "Visuals", vis_dir, col_dir, lname, urdf_root_dir):
            link_el.append(v_el)
        for c_el in await build_visual_or_collision(lnode, "Collisions", vis_dir, col_dir, lname, urdf_root_dir):
            link_el.append(c_el)

        links_el.append(link_el)
    return links_el


# ----------------- Joint-Typ ableiten -----------------
def joint_type_from_mp(mp_val: Optional[int], joint_type_prop: Optional[str]) -> str:
    if mp_val == 1:   return "revolute"
    if mp_val == 2:   return "continuous"
    if mp_val == 3:   return "prismatic"
    # Fallbacks
    if joint_type_prop and joint_type_prop.strip():
        jt = joint_type_prop.strip().lower()
        if jt in ("fixed","planar","floating","revolute","continuous","prismatic"):
            return jt
    return "fixed"

# ----------------- Axes + AdditionalJoints → URDF joints -----------------
async def read_joints(device_node) -> List[ET.Element]:
    joints_el = []
    axes_folder = await get_child_by_name(device_node, "Axes")
    if not axes_folder:
        return joints_el

    # Sammle alle Achsknoten außer dem "AdditionalJoints" Ordner
    axis_nodes = []
    add_folder = None
    for name, ch in await list_children(axes_folder):
        if name == "AdditionalJoints":
            add_folder = ch
            continue
        # nur Objekte namens "Axis*"
        if name.startswith("Axis") and await ch.read_node_class() == ua.NodeClass.Object:
            axis_nodes.append(ch)

    # Helper: joint aus einem Axis-Objekt bauen
    async def joint_from_axis(axis_node) -> Optional[ET.Element]:
        jname  = await get_child_value(axis_node, "Name", None)
        parent = await get_child_value(axis_node, "ParentLink", None)
        child  = await get_child_value(axis_node, "ChildLink", None)
        if not jname or not parent or not child:
            return None

        mp_val = await get_child_value(axis_node, "MotionProfile", None)
        jt_prop = await get_child_value(axis_node, "JointType", None)
        jtype = joint_type_from_mp(mp_val, jt_prop)

        j_el = ET.Element("joint", {"name": str(jname), "type": jtype})

        # parent/child
        _add_child(j_el, "parent", link=parent)
        _add_child(j_el, "child",  link=child)

        # origin
        origin_node = await get_child_by_name(axis_node, "Origin")
        if origin_node:
            xyz = await get_child_value(origin_node, "xyz", None)
            rpy = await get_child_value(origin_node, "rpy", None)
            o_el = _add_child(j_el, "origin")
            _set_attr_if(o_el, "xyz", xyz)
            _set_attr_if(o_el, "rpy", rpy)

        # axis
        axis_xyz = await get_child_value(axis_node, "Axis_xyz", None)
        if axis_xyz:
            _add_child(j_el, "axis", xyz=_fmt_xyz(axis_xyz))

        # Limit
        limit_node = await get_child_by_name(axis_node, "Limit")
        if limit_node:
            lower   = await get_child_value(limit_node, "lower",   None)
            upper   = await get_child_value(limit_node, "upper",   None)
            vel     = await get_child_value(limit_node, "velocity",None)
            effort  = await get_child_value(limit_node, "effort",  None)
            if any(v is not None for v in (lower, upper, vel, effort)):
                _add_child(j_el, "limit",
                           lower=lower, upper=upper,
                           velocity=vel, effort=effort)

        # Dynamics
        dyn = await get_child_by_name(axis_node, "Dynamics")
        if dyn:
            damping  = await get_child_value(dyn, "damping",  None)
            friction = await get_child_value(dyn, "friction", None)
            if damping is not None or friction is not None:
                _add_child(j_el, "dynamics", damping=damping, friction=friction)

        # Calibration
        cal = await get_child_by_name(axis_node, "Calibration")
        if cal:
            rising  = await get_child_value(cal, "rising",  None)
            falling = await get_child_value(cal, "falling", None)
            if rising is not None or falling is not None:
                _add_child(j_el, "calibration", rising=rising, falling=falling)

        # SafetyController
        sc = await get_child_by_name(axis_node, "SafetyController")
        if sc:
            sll = await get_child_value(sc, "soft_lower_limit", None)
            sul = await get_child_value(sc, "soft_upper_limit", None)
            kv  = await get_child_value(sc, "k_velocity", None)
            kp  = await get_child_value(sc, "k_position", None)
            if any(v is not None for v in (sll, sul, kv, kp)):
                _add_child(j_el, "safety_controller",
                           soft_lower_limit=sll, soft_upper_limit=sul,
                           k_velocity=kv, k_position=kp)

        # Mimic
        mim = await get_child_by_name(axis_node, "Mimic")
        if mim:
            m_joint  = await get_child_value(mim, "joint",      None)
            m_multi  = await get_child_value(mim, "multiplier", None)
            m_offset = await get_child_value(mim, "offset",     None)
            if m_joint is not None:
                _add_child(j_el, "mimic", joint=m_joint,
                           multiplier=m_multi if m_multi is not None else None,
                           offset=m_offset if m_offset is not None else None)

        return j_el

    # reguläre Achsen
    for ax in axis_nodes:
        j_el = await joint_from_axis(ax)
        if j_el is not None:
            joints_el.append(j_el)

    # AdditionalJoints
    if add_folder:
        for name, ax in await list_children(add_folder):
            if await ax.read_node_class() != ua.NodeClass.Object:
                continue
            if not name.startswith("Axis"):
                continue
            j_el = await joint_from_axis(ax)
            if j_el is not None:
                joints_el.append(j_el)

    return joints_el

# ----------------- Hauptfunktion: OPC UA → URDF -----------------
async def export_urdf_from_server(endpoint: str, out_urdf_path: str):
    client = Client(endpoint)
    await client.connect()
    try:
        device = await find_motion_device(client)
        if not device:
            raise RuntimeError("Kein MotionDevice im Server gefunden.")

        urdf_name = await get_child_value(device, "URDFName", None)
        if not urdf_name:
            bn = await device.read_browse_name()
            urdf_name = bn.Name or "robot"

        # Ordner & endgültigen Pfad vorbereiten
        urdf_root_dir, out_urdf_final, vis_dir, col_dir = _prepare_urdf_root(out_urdf_path, urdf_name)

        robot_el = ET.Element("robot", {"name": urdf_name})

        # Links (mit Asset-Export in urdf/meshes/*)
        for l_el in await read_links(device, vis_dir, col_dir, urdf_root_dir):
            robot_el.append(l_el)

        # Joints (unverändert)
        for j_el in await read_joints(device):
            robot_el.append(j_el)

        ET.indent(ET.ElementTree(robot_el), space="  ", level=0)
        ET.ElementTree(robot_el).write(out_urdf_final, encoding="utf-8", xml_declaration=True)

        print(f"URDF:    {out_urdf_final}")
        print(f"Meshes:  {os.path.join(urdf_root_dir, 'meshes')}")
    finally:
        await client.disconnect()


# ----------------- CLI-Beispiel -----------------
if __name__ == "__main__":
    import argparse, sys

    parser = argparse.ArgumentParser(
        description="Exportiert URDF + Meshes aus einem laufenden OPC UA Server."
    )
    parser.add_argument(
        "endpoint",
        help="OPC UA Endpoint, z.B. opc.tcp://127.0.0.1:4840",
    )
    parser.add_argument(
        "-o", "--out",
        dest="out_urdf",
        default="recovered_robot.urdf",
        help=("Ziel-URDF-Pfad (Datei oder Basisname). "
              "Die Ausgaben landen in <robot>_description/ mit meshes/visuals und meshes/collisions. "
              "Standard: recovered_robot.urdf im aktuellen Verzeichnis."),
    )

    args = parser.parse_args()
    try:
        asyncio.run(export_urdf_from_server(args.endpoint, args.out_urdf))
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)


        # Verwendung

        # Standard: schreibt recovered_robot.urdf und erzeugt <robot>_description/...
        # python Transformer_OPCUA_2_URDF.py opc.tcp://127.0.0.1:4840

        # # Mit explizitem Zielpfad/Dateiname
        # python Transformer_OPCUA_2_URDF.py opc.tcp://127.0.0.1:4840 -o C:\pfad\zu\out\my_robot.urdf