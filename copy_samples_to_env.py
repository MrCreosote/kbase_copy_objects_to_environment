"""
VERY Q&D script to copy sample information for objects from one KBase environment to another.

Requires direct access to all the objects involved.
"""

import json
from pathlib import Path
import random
import requests
import sys

# TODO optionally use admin perms

"""
Issues:

* hardcoded params
* Only works for workspaces with < 10000 objects of the type of interest
* massively inefficient
* no error handling
"""


SOURCE_ENV="https://kbase.us/services/"
TARGET_ENV="https://ci.kbase.us/services/"

SOURCE_WS = 106867 # GROW
TARGET_WS = 69037
TYPE = "KBaseGenomes.Genome"

WS = "ws"
SAMPLE_SERVICE = "sampleservice"

CLI_WS = "ws"
CLI_SAMPLE = "sample"
SOURCE = "source"
TARGET = "target"

COPY_SOURCE_UPA = "copy_source_upa"


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


def to_upa(objinf):
    return f"{objinf[6]}/{objinf[0]}/{objinf[4]}"


def get_token(token_file):
    with open(token_file) as f:
        return f.read().strip()


def get_clients(source_token_file, target_token_file):
    source_token = get_token(source_token_file)
    target_token = get_token(target_token_file)
    return {
        SOURCE: {
            CLI_WS: CrapSDKClient(SOURCE_ENV + WS, "Workspace", source_token),
            CLI_SAMPLE: CrapSDKClient(SOURCE_ENV + SAMPLE_SERVICE, "SampleService", source_token),
        },
        TARGET: {
            CLI_WS: CrapSDKClient(TARGET_ENV + WS, "Workspace", target_token),
            CLI_SAMPLE: CrapSDKClient(TARGET_ENV + SAMPLE_SERVICE, "SampleService", target_token),
        }
    }


def main():
    samples_completed_file = Path(sys.argv[1])
    source_token_file = sys.argv[2]
    target_token_file = sys.argv[3]
    clients = get_clients(source_token_file, target_token_file)

    completed_samples = {}
    if samples_completed_file.exists():
        with open(samples_completed_file) as f:
            for l in f:
                source, srcver, target, tarver = l.split("\t")
                completed_samples[(source.strip(), int(srcver.strip()))] = (
                    target.strip(), int(tarver.strip())
                )
    target_objs = clients[TARGET][CLI_WS].call(
        "list_objects", {"ids": [TARGET_WS], "type": TYPE, "includeMetadata": 1})
    count = 1
    total = len(target_objs)
    for o in target_objs:
        source_upa = o[10][COPY_SOURCE_UPA]
        print(f"Processing object {count}/{total}: {to_upa(o)}, source: {source_upa}")
        links = clients[SOURCE][CLI_SAMPLE].call(
            "get_data_links_from_data", {"upa": source_upa}
        )["links"]
        for l in links:
            if l["version"] != 1:
                raise ValueError(
                    "This script is too stupid to deal with sample versions right now: " + l)
                    # Gets hairy if objects refer to the different versions of the same sample
                    # Also difficult to map to an existing version stack of samples - which version
                    # to map to?
            existing_sample = completed_samples.get((l["id"], l["version"]))
            if existing_sample:
                print(f"\tUsing existing copy of sample {l['id']}/{l['version']}: "
                      + f"{existing_sample[0]}/{existing_sample[1]}")
            else:
                sample = clients[SOURCE][CLI_SAMPLE].call(
                    "get_sample_via_data",
                    {
                        "upa": source_upa,
                        "id": l["id"],
                        "version": l["version"]
                    }
                )
                source_id = sample.pop("id")
                # This will always create a new sample. We assume the samples don't already
                # exist, as there's no way in the sample service to find them.
                # Maybe search? but even then how would you know they're equivalent?
                # Hence the concordance file
                newsample = clients[TARGET][CLI_SAMPLE].call("create_sample", {"sample": sample})
                with open(samples_completed_file, "a") as f:
                    f.write("\t".join(
                        [
                            source_id, str(sample["version"]),
                            newsample["id"], str(newsample["version"])
                        ]
                    ) + "\n")
                completed_samples[(source_id, sample["version"])] = (
                    newsample["id"], newsample["version"])
                print(f"\tCopied sample {source_id}/{sample['version']} to "
                      + f"{newsample['id']}/{newsample['version']}")
                existing_sample = (newsample['id'], newsample['version'])
            new_link = clients[TARGET][CLI_SAMPLE].call(
                "create_data_link",
                {
                    "upa": to_upa(o),
                    "dataid": None,
                    "id": existing_sample[0],
                    "version": existing_sample[1],
                    "node": l["node"],
                    "update": True,
                }
            )["new_link"]
            print("\tCreated link:")
            for key in sorted(new_link):
                print(f"\t\t{key}: {new_link[key]}")
        count += 1


if __name__ == "__main__":
    main()
