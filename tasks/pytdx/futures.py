from pytdx.exhq import TdxExHq_API
api = TdxExHq_API()
with api.connect('101.227.77.254', 7727):
    api.get_markets()
    api.get_minute_time_data(47, "IF1709")

