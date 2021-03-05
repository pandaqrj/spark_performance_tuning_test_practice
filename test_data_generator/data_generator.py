from faker import Faker
import json

if __name__ == '__main__':
    fake = Faker(locale="zh_CN")
    order = {
        'order_id': ''
        , 'province': ''
        , 'amount': 0
        , 'date': ''
    }
    i = 1
    f = open(file="../data3.json", mode="w", encoding="utf-8")
    while i < 1000000:
        order['order_id'] = str(fake.ean8())
        order['province'] = fake.province()
        order['amount'] = fake.random_int(min=10, max=10000)
        order['date'] = str(fake.date_between(start_date='-10y', end_date='today'))
        jsstr = json.dumps(order, ensure_ascii=False)
        f.write(f"{jsstr}\n")
        if i % 1000 == 0:
            print(i)
        i += 1
