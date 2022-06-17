import json, requests

class beeline:
    def __init__(self, token):
        self.token = token

    def get_statistics(self, date_from, date_to):
        result = []
        index = 0
        while True:
            headers = {
                'X-MPBX-API-AUTH-TOKEN': self.token,
            }

            params = (
                ('dateFrom', date_from),
                ('dateTo', date_to),
                ('page', index),
                ('pageSize', '100'),
            )
            print(index)
            response = requests.get('https://cloudpbx.beeline.ru/apis/portal/v2/statistics', headers=headers, params=params).text
            items = json.loads(response)
            if len(items)>0:
                for item in items:
                    result.append(item)
                index+=1
            else:
                break
        return result

    def get_records_list(self, date_from, date_to):

        headers = {
            'X-MPBX-API-AUTH-TOKEN': self.token,
        }

        params = (
            ('dateFrom', date_from),
            ('dateTo', date_to)
        )

        response = requests.get('https://cloudpbx.beeline.ru/apis/portal/records', headers=headers, params=params).text

        items = json.loads(response)
        return items

    def get_record_url(self, id):
        headers = {
            'X-MPBX-API-AUTH-TOKEN': self.token,
        }


        response = requests.get('https://cloudpbx.beeline.ru/apis/portal/records/'+str(id)+'/reference', headers=headers).text
        return json.loads(response)['url']

    def get_record_id_by_user(self, date_from, date_to, userid, phone, direction, duration):

        headers = {
            'X-MPBX-API-AUTH-TOKEN': self.token,
        }

        params = (
            ('dateFrom', date_from),
            ('dateTo', date_to),
            ('userId', userid)
        )

        response = requests.get('https://cloudpbx.beeline.ru/apis/portal/records', headers=headers, params=params).text

        items = json.loads(response)
        for i, item in enumerate(items):

            if item['phone']!= phone or item['direction'] != direction:
                items.pop(i)


        if len(items)>1:
            tmp = []
            for item in items:
                tmp.append(abs(int(item['duration'])-duration))
            items = [items[tmp.index(min(tmp))]]

        print(userid, date_from, date_to, len(items), items)
        if len(items) > 0:
            return self.get_record_url(items[0]['id'])
        return None



