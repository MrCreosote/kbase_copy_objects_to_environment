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

SOURCE_WS = 41372
TARGET_WS = 68999

# Copy genomes and their respective assemblies. If False, only copy assemblies.
GENOMES = True

# TODO need retries, getting connection aborted errors from hids_to_handles and getting the blobstore node

WS = "ws"
HANDLE = "handle_service"
BLOBSTORE = "shock-api"

# WARNING - the script is purposely written for this type and won't work for other types
ASS_TYPE = "KBaseGenomeAnnotations.Assembly"
GEN_TYPE = "KBaseGenomes.Genome"

# need to change this to functions that take the field & object and mutate the object
REMOVE_OBJECT_FIELDS = {
    ASS_TYPE: ["fasta_handle_info"],  # in assembly but not in assembly type
    GEN_TYPE: [
        "taxon_ref",  # deprecated, don't bother with translating
        "ontology_events",  # update per https://kbase.slack.com/archives/C4E7KUGTD/p1682388280309109
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


def save_object(cli, obj):
    return cli.call(
        "save_objects",
        {
            "id": TARGET_WS,
            "objects": [{
                "name": obj["info"][1],
                "type": obj["info"][2],
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


def main():
    active_type = GEN_TYPE if GENOMES else ASS_TYPE

    source_token_file = sys.argv[1]
    target_token_file = sys.argv[2]
    clients = get_clients(source_token_file, target_token_file)

    target_objs = clients[TARGET][CLI_WS].call(
        "list_objects", {"ids": [TARGET_WS], "type": active_type})
    target_completed_names = {o[1] for o in target_objs}
    source_objs = clients[SOURCE][CLI_WS].call(
        "list_objects", {"ids": [SOURCE_WS], "type": active_type})
    source_names = {o[1] for o in source_objs}
    todo_names = source_names - target_completed_names
    total = len(todo_names)
    print(f"{total} objects to process out of {len(source_names)} total objects")
    todo = [o for o in source_objs if o[1] in todo_names]
    allstart = time.time()
    count = 1
    assynames = {}
    for sourceobj in todo:
        start = time.time()
        upa = to_upa(sourceobj)
        print(f"Processing #{count}/{total}, {upa}, {sourceobj[1]}, {sourceobj[2]}")
        obj = get_object(clients[SOURCE][CLI_WS], upa)
        obj["info"][2] = map_type_to_target(clients, obj["info"][2])
        for field in REMOVE_OBJECT_FIELDS[active_type]:
            obj["data"].pop(field, None)
        if GENOMES:
            assyupa = obj["data"]["assembly_ref"]
            assy = get_object(clients[SOURCE][CLI_WS], f"{upa};{assyupa}")
            name = assy["info"][1]
            # could be in multiple workspaces = no unique name guarantee
            name = f"{name}_{assyupa}" if name in assynames else name
            assy["info"][1] = name
            info = clients[TARGET][CLI_WS].call(  # could call info on the source before get obj
                "get_object_info3",
                {"objects": [{"ref": f"{TARGET_WS}/{name}"}], "ignoreErrors": 1}
            )["infos"][0]
            if info:
                assyupa_target = to_upa(info)
                print(f"\tFound existing assembly {name} {assyupa_target}")
            else:
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
                info = save_object(clients[TARGET][CLI_WS], assy)
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
        objinf = save_object(clients[TARGET][CLI_WS], obj)
        print(
            f"\tSaved object {to_upa(objinf)} {objinf[2]} {objinf[9]} in {time.time() - genstart}")
        print(f"\tElapsed time: {time.time() - start} Total time: {time.time() - allstart}",
            flush=True)
        count += 1


if __name__ == "__main__":
    main()
