from faker import Faker

if __name__ == '__main__':
    fake = Faker(locale="zh_CN")
    i = 1
    f = open(file="../data3.txt", mode="w", encoding="utf-8")
    while i < 100000000:
        output_str = f"{str(fake.ean13())}\t{fake.province()}\t{fake.random_int(min=10, max=10000)}\t{str(fake.date_between(start_date='-10y', end_date='today'))}"
        f.write(f"{output_str}\n")
        if i % 10000 == 0:
            print(i)
        i += 1
