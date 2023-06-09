import json
import os
import random
import requests
import sys
import tempfile
import time

"""
Issues:

* hardcoded params
* only works for Assemblies and Genomes
  * Getting this to work for any object would be a *ton* of work
  * Also, if there are environment specific fields that the workspace doesn't understand
    (like blobstore nodes shoved into an object with a handle) they won't get updated
* Only works for workspaces with < 10000 objects of the type of interest
* Ignores hidden objects (which might be ok)
* massively inefficient
* no error handling
"""

SOURCE_ENV="https://kbase.us/services/"
TARGET_ENV="https://ci.kbase.us/services/"

SOURCE_WS = 106867 # GROW
TARGET_WS = 69037

# Copy genomes and their respective assemblies. If False, only copy assemblies.
GENOMES = True

# TODO need retries, getting connection aborted errors from hids_to_handles and getting the blobstore node


WS = "ws"
HANDLE = "handle_service"
BLOBSTORE = "shock-api"

# WARNING - the script is purposely written for these types and won't work for other types
ASS_TYPE = "KBaseGenomeAnnotations.Assembly"
GEN_TYPE = "KBaseGenomes.Genome"

_ONTOLOGY_EVENTS = "ontology_events"

COPY_SOURCE_UPA = "copy_source_upa"
COPY_SOURCE_URL = "copy_source_url"


def _remove_onto(obj):
    newonto = []
    if _ONTOLOGY_EVENTS in obj:
        for o in obj[_ONTOLOGY_EVENTS]:
            # https://kbase.slack.com/archives/C4E7KUGTD/p1682388280309109
            if "ontology_ref" not in o:
                newonto.append(o)
        obj[_ONTOLOGY_EVENTS] = newonto

# need to change this to functions that take the field & object and mutate the object
ALTER_OBJECT_FIELDS = {
    ASS_TYPE: [
        lambda obj: obj.pop("fasta_handle_info", None),
    ],  # in assembly but not in assembly type
    GEN_TYPE: [
        lambda obj: obj.pop("taxon_ref", None),  # deprecated, don't bother with translating
        _remove_onto,
    ],
}

CLI_WS = "ws"
CLI_HANDLE = "handle"
CLI_BLOBSTORE = "blobstore"
SOURCE = "source"
TARGET = "target"

# source to target
TYPE_MAPPING_CACHE = {}


class CrapSDKClient:

    def __init__(self, url, name, token):
        self._url = url
        self._name = name
        self._headers = {"AUTHORIZATION": token}
        self._session = requests.Session()

    def call(self, method, params):
        body = {
                'method': f"{self._name}.{method}",
                'params': [params],
                'version': '1.1',
                'id': str(random.random())[2:]
                }
        with self._session.post(self._url, headers=self._headers, json=body) as res:
            j = res.json()
            if "result" in j:
                return j["result"][0]
            else:
                print(json.dumps(j, indent=4))
                res.raise_for_status()


class CrapBlobStoreClient:

    def __init__(self, url, token):
        self._url = url
        self._headers = {"AUTHORIZATION": f"OAuth {token}"}
        self._session = requests.Session()

    def get_node(self, node_id):
        with self._session.get(f"{self._url}/node/{node_id}", headers=self._headers) as res:
            res.raise_for_status()
            return res.json()['data']

    def get_file(self, node_id, to_file):
        url = f"{self._url}/node/{node_id}/?download"
        with self._session.get(url, stream=True, headers=self._headers) as res:
            res.raise_for_status()
            with open(to_file, 'wb') as f:
                for chunk in res.iter_content(chunk_size=10 * 1024 * 1024): 
                    f.write(chunk)

    def create_node(self, filename, file_like_object):
        with self._session.post(
            f"{self._url}/node/?filename={filename}",
            headers=self._headers,
            data=file_like_object
        ) as res:
            res.raise_for_status()
            return res.json()["data"]


def get_token(token_file):
    with open(token_file) as f:
        return f.read().strip()


def to_upa(objinf):
    return f"{objinf[6]}/{objinf[0]}/{objinf[4]}"


def get_clients(source_token_file, target_token_file):
    source_token = get_token(source_token_file)
    target_token = get_token(target_token_file)
    return {
        SOURCE: {
            CLI_WS: CrapSDKClient(SOURCE_ENV + WS, "Workspace", source_token),
            CLI_HANDLE: CrapSDKClient(SOURCE_ENV + HANDLE, "AbstractHandle", source_token),
            CLI_BLOBSTORE: CrapBlobStoreClient(SOURCE_ENV + BLOBSTORE, source_token)
        },
        TARGET: {
            CLI_WS: CrapSDKClient(TARGET_ENV + WS, "Workspace", target_token),
            CLI_HANDLE: CrapSDKClient(TARGET_ENV + HANDLE, "AbstractHandle", target_token),
            CLI_BLOBSTORE: CrapBlobStoreClient(TARGET_ENV + BLOBSTORE, target_token)
        }
    }


def get_object(cli, upa):
    return cli.call("get_objects2", {"objects": [{"ref": upa}]})["data"][0]


def save_object(cli, obj, meta):
    return cli.call(
        "save_objects",
        {
            "id": TARGET_WS,
            "objects": [{
                "name": obj["info"][1],
                "type": obj["info"][2],
                "meta": meta,
                "data": obj["data"],
                "provenance": [{
                    "description": f"Copied from {SOURCE_ENV + WS} {to_upa(obj['info'])}"
                }]
            }]
        }    
    )[0]
    

def transfer_file(clients, hid, prefix=""):
    handle = clients[SOURCE][CLI_HANDLE].call("hids_to_handles", [hid])[0]
    blobstore_id = handle["id"]
    node = clients[SOURCE][CLI_BLOBSTORE].get_node(blobstore_id)
    file_name = node["file"]["name"]
    file_size = node["file"]["size"]
    print(f"{prefix}\tDownloading file {hid} {blobstore_id} {file_name} {file_size}... ",
        end="", flush=True)
    h, tmpf = tempfile.mkstemp()
    try:
        os.close(h)
        clients[SOURCE][CLI_BLOBSTORE].get_file(blobstore_id, tmpf)
        print("done. Uploading file... ", end="", flush=True)
        with open(tmpf, "rb") as f:
            target_node = clients[TARGET][CLI_BLOBSTORE].create_node(file_name, f)
            # could check the md5 matches here
        print("done.", flush=True)
    finally:
        os.remove(tmpf)
    hid = clients[TARGET][CLI_HANDLE].call(
        "persist_handle",
        {
            "id": target_node["id"],
            "filename": file_name,
            "type": "shock",
            "url": TARGET_ENV + BLOBSTORE,
            "remote_md5": target_node["file"]["checksum"]["md5"],
        }
    )
    print(f"{prefix}\t{hid} {target_node['id']}")
    return hid


def map_type_to_target(clients, source_type):
    if source_type in TYPE_MAPPING_CACHE:
        return TYPE_MAPPING_CACHE[source_type]
    md5type = clients[SOURCE][CLI_WS].call("translate_to_MD5_types", [source_type])[source_type]
    target_types = clients[TARGET][CLI_WS].call('translate_from_MD5_types', [md5type])[md5type]
    if not target_types:
        # punt to latest
        newtype = source_type.split('-')[0]
        TYPE_MAPPING_CACHE[source_type] = newtype
        return newtype
    tt = [tuple(map(int, t.split('-')[1].split('.'))) for t in target_types]
    maxver = sorted(tt)[-1]
    newtype = f"{source_type.split('-')[0]}-{maxver[0]}.{maxver[1]}"
    TYPE_MAPPING_CACHE[source_type] = newtype
    return newtype


def _update_fields_in_place(obj, type_):
    for fn in ALTER_OBJECT_FIELDS[type_]:
        fn(obj)    


def _find_copy(clients, source_upa):
    objs = clients[TARGET][CLI_WS].call(
        "list_objects",
        {"ids": [TARGET_WS], "meta": {COPY_SOURCE_UPA: source_upa}, "includeMetadata": 1}
    )
    url = SOURCE_ENV + WS
    for o in objs:
        if url == o[10][COPY_SOURCE_URL]:
            return o
    return None


def main():
    active_type = GEN_TYPE if GENOMES else ASS_TYPE

    source_token_file = sys.argv[1]
    target_token_file = sys.argv[2]
    clients = get_clients(source_token_file, target_token_file)

    target_objs = clients[TARGET][CLI_WS].call(
        "list_objects", {"ids": [TARGET_WS], "type": active_type, "includeMetadata": 1})
    source_completed_upas = {o[10][COPY_SOURCE_UPA] for o in target_objs}
    source_objs = clients[SOURCE][CLI_WS].call(
        "list_objects", {"ids": [SOURCE_WS], "type": active_type})
    todo_objects = [o for o in source_objs if to_upa(o) not in source_completed_upas]
    total = len(todo_objects)
    print(f"{total} objects to process out of {len(source_objs)} total objects")
    allstart = time.time()
    count = 1
    assynames = {}
    for sourceobj in todo_objects:
        start = time.time()
        upa = to_upa(sourceobj)
        print(f"Processing #{count}/{total}, {upa}, {sourceobj[1]}, {sourceobj[2]}")
        obj = get_object(clients[SOURCE][CLI_WS], upa)
        obj["info"][2] = map_type_to_target(clients, obj["info"][2])
        _update_fields_in_place(obj["data"], active_type)
        if GENOMES:
            assyupa = obj["data"]["assembly_ref"]
            copy = _find_copy(clients, assyupa)
            if copy:
                assyupa_target = to_upa(copy)
                print(f"\tFound existing assembly {copy[1]} {assyupa_target}")
            else:
                assy = get_object(clients[SOURCE][CLI_WS], f"{upa};{assyupa}")
                _update_fields_in_place(assy["data"], ASS_TYPE)
                name = assy["info"][1]
                # could be in multiple workspaces = no unique name guarantee
                name = f"{name}_{assyupa}" if name in assynames else name
                assy["info"][1] = name
                if assy["info"][2].split('-')[0] != ASS_TYPE:
                    raise ValueError(f"Genome {upa} assembly {assyupa} is type {assy['info'][2]}")
                print(f"\tProcessing assembly {assyupa} {assy['info'][1]} {assy['info'][2]}")
                # note this does not copy "reads_handle_ref", which IMO, shouldn't be in the assy
                # object anyway. It should point to a reads object in the provenance instead
                assy["info"][2] = map_type_to_target(clients, assy["info"][2])
                hid = transfer_file(clients, assy["data"]["fasta_handle_ref"], "\t")
                assy["data"]["fasta_handle_ref"] = hid
                print(f"\t\tSaving object as type {assy['info'][2]}")
                assystart = time.time()
                info = save_object(
                    clients[TARGET][CLI_WS],
                    assy,
                    {COPY_SOURCE_UPA: assyupa, COPY_SOURCE_URL: SOURCE_ENV + WS}
                )
                assyupa_target = to_upa(info)
                print(f"\t\tSaved assembly object {assyupa_target} {info[9]} in "
                        + f"{time.time() - assystart}")
            obj["data"]["assembly_ref"] = assyupa_target
        print(f"\tProcessing files for object")
        # may need to split these up by type
        for field in ["genbank_handle_ref", "gff_handle_ref", "fasta_handle_ref"]:
            if obj["data"].get(field):
                hid = transfer_file(clients, obj["data"][field])
                obj["data"][field] = hid
        print(f"\tSaving object as type {obj['info'][2]}")
        genstart = time.time()
        objinf = save_object(
            clients[TARGET][CLI_WS],
            obj,
            {COPY_SOURCE_UPA: upa, COPY_SOURCE_URL: SOURCE_ENV + WS})
        print(
            f"\tSaved object {to_upa(objinf)} {objinf[2]} {objinf[9]} in {time.time() - genstart}")
        print(f"\tElapsed time: {time.time() - start} Total time: {time.time() - allstart}",
            flush=True)
        count += 1


if __name__ == "__main__":
    main()
