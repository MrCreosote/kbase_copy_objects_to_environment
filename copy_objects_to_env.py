import os
import random
import requests
import sys
import tempfile
import time

"""
Issues:

* hardcoded params
* only works for Assemblies
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

SOURCE_WS = 124291
TARGET_WS = 68981

WS = "ws"
HANDLE = "handle_service"
BLOBSTORE = "shock-api"

# WARNING - the script is purposely written for this type and won't work for other types
TYPE = "KBaseGenomeAnnotations.Assembly"

REMOVE_OBJECT_FIELDS = ["fasta_handle_info"]  # in assembly but not in assembly type


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
            return res.json()["result"][0]


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

def main():
    source_token_file = sys.argv[1]
    target_token_file = sys.argv[2]
    source_token = get_token(source_token_file)
    target_token = get_token(target_token_file)

    source_ws = CrapSDKClient(SOURCE_ENV + WS, "Workspace", source_token)
    target_ws = CrapSDKClient(TARGET_ENV + WS, "Workspace", target_token)
    source_hndl = CrapSDKClient(SOURCE_ENV + HANDLE, "AbstractHandle", source_token)
    target_hndl = CrapSDKClient(TARGET_ENV + HANDLE, "AbstractHandle", target_token)
    source_bs = CrapBlobStoreClient(SOURCE_ENV + BLOBSTORE, source_token)
    target_bs = CrapBlobStoreClient(TARGET_ENV + BLOBSTORE, target_token)

    target_objs = target_ws.call("list_objects", {"ids": [TARGET_WS], "type": TYPE})
    target_completed_names = {o[1] for o in target_objs}
    source_objs = source_ws.call("list_objects", {"ids": [SOURCE_WS], "type": TYPE})
    source_names = {o[1] for o in source_objs}
    todo_names = source_names - target_completed_names
    total = len(todo_names)
    print(f"{total} objects to process out of {len(source_names)} total objects")
    todo = [o for o in source_objs if o[1] in todo_names]
    allstart = time.time()
    count = 1
    for sourceobj in todo:
        start = time.time()
        upa = to_upa(sourceobj)
        obj = source_ws.call("get_objects2", {"objects": [{"ref": upa}]})["data"][0]
        for field in REMOVE_OBJECT_FIELDS:
            obj["data"].pop(field, None)
        hid = obj["extracted_ids"]["handle"][0]
        handle = source_hndl.call("hids_to_handles", [hid])[0]
        blobstore_id = handle["id"]
        node = source_bs.get_node(blobstore_id)
        filename = node["file"]["name"]
        size = node["file"]["size"]
        print(f"Processing #{count}/{total}, {upa}:\n\t{sourceobj[1]}\n\t{hid}\n\t{blobstore_id}")
        print(f"\t{filename}\n\t{size}")
        print("\tDownloading file... ", end="", flush=True)
        h, tmpf = tempfile.mkstemp()
        try:
            os.close(h)
            source_bs.get_file(blobstore_id, tmpf)
            print("done. Uploading file... ", end="", flush=True)
            with open(tmpf, "rb") as f:
                target_node = target_bs.create_node(filename, f)
            print("done.", flush=True)
        finally:
            os.remove(tmpf)
        hid = target_hndl.call(
            "persist_handle",
            {
                "id": target_node["id"],
                "filename": filename,
                "type": "shock",
                "url": TARGET_ENV + BLOBSTORE,
                "remote_md5": target_node["file"]["checksum"]["md5"],
            }
        )
        obj["data"]["fasta_handle_ref"] = hid
        objinf = target_ws.call(
            "save_objects",
            {
                "id": TARGET_WS,
                "objects": [{
                    "name": sourceobj[1],
                    "type": TYPE,
                    "data": obj["data"],
                    "provenance": [{"description": f"Copied from {SOURCE_ENV + WS} {upa}"}]
                }]
            }    
        )[0]
        print(f"\tSaved object {to_upa(objinf)} {hid} {target_node['id']}")
        print(f"\tElapsed time: {time.time() - start} Total time: {time.time() - allstart}",
            flush=True)
        count += 1


if __name__ == "__main__":
    main()
