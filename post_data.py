import requests
from requests.exceptions import RequestException


class PostData:
    select_url = "https://data-backend.banggood.cn/crawler/gettasks"
    update_url = "https://data-backend.banggood.cn/crawler/updatetask"

    def select(self, **kwargs):
        """
        :param kwargs:{"wtcPlatform": "amazon", "wtcStatus": 2, "limit":10}
        :return:
        """
        try:
            req = requests.post(self.select_url, data=kwargs)
            if req.status_code == 200:
                result_dict = req.json()
                if result_dict["status"] == 1:
                    return result_dict["data"]
                else:
                    print("select fail: {!r}".format(result_dict))
            else:
                print("select code: {!r}".format(req.status_code))
        except RequestException as err:
            print("select exception: {!r}".format(err))

    def update(self, **kwargs):
        """
        :param kwargs: {"wtcStatus": -1, "wtcId"：1}
        :return:
        """
        try:
            req = requests.post(self.update_url, data=kwargs)
            if req.status_code == 200:
                result_dict = req.json()
                if result_dict["status"] == 1:
                    print(result_dict["desc"])
                else:
                    print("update fail: {!r}".format(result_dict))
            else:
                print("update code: {!r}".format(req.status_code))
        except RequestException as err:
            print("update exception: {!r}".format(err))


if __name__ == "__main__":
    pd = PostData()
    print(pd.select(wtcPlatform="amazon", wtcStatus=0, limit=1))
    pd.update(wtcStatus=2, wtcId=1)
