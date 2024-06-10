s = "Host=10.10.12.17;Port=5432;Database=2300104684;User ID=minvoice;Password=Minvoice@123;"


def get_info(host_slave, item: str) -> dict:
    res = {}

    for item in item.split(';')[:-1]:
        key, value = item.split('=')
        res[key] = value

    res["Host"] = host_slave
    return res


print(get_info(host_slave="10.10.12.18", item=s))
