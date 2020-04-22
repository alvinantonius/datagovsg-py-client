import requests
from typing import List


class Client(object):
    def __init__(self, host="https://data.gov.sg"):
        self.host = host

    def listDataset(self) -> List[str]:
        response = requests.get("{}/api/action/package_list".format(self.host))
        response.json()["result"]

    def GetDatasetByName(self, name):
        response = requests.get("{}/api/action/package_show?id={}".format(self.host, name))
        if response.status_code != 200:
            raise RequestUnsuccessful
        res = response.json()
        if res['success'] and res["result"]:
            return DataSet(res["result"], self.host)
        else:
            raise DataSetNotFound


class DataSet(object):
    def __init__(self, dataset_info, host):
        self.dataset_info = dataset_info
        self.name = dataset_info["name"]
        self.id = dataset_info["id"]
        self.host = host

        self.resources = []
        for res in dataset_info["resources"]:
            self.resources.append(Resource(res, self.host))

    def getResources(self):
        return self.resources


class Resource(object):
    def __init__(self, resource_info, host):
        self.resource_info = resource_info
        self.name = resource_info["name"]
        self.id = resource_info["id"]
        self.url = resource_info["url"]
        self.format = resource_info["format"]
        self.dataset_id = resource_info["package_id"]
        self.host = host

    def downloadFile(self, target_filename):
        downloadedfile = requests.get(self.url)
        open(target_filename, "wb").write(downloadedfile.content)

    def fetchData(self, take=100, skip=0):
        res = requests.get("{}/api/action/datastore_search?resource_id={}&limit={}&offset={}".
                           format(self.host, self.id, take, skip))
        if res.status_code != 200:
            raise RequestUnsuccessful
        return res.json()

    def fetchAllData(self):
        results = []

        initialFetch = self.fetchData(100, 0)
        results.append(initialFetch["result"]["records"])
        nextFetchURL = self.host + initialFetch["result"]["_links"]["next"]
        while True:
            res = requests.get(nextFetchURL).json()
            if len(res["result"]["records"]) == 0:
                break
            results.append(res["result"]["records"])
        return results


class DataSetNotFound(Exception):
    """Unable to find dataset"""
    pass


class RequestUnsuccessful(Exception):
    """Got non 200 response from host"""
    pass
