from faker import Faker


if __name__ == '__main__':
    fake = Faker()
    f = open(file="../data4.txt", mode="w", encoding="utf-8")
    f.write(fake.text(max_nb_chars=10000))
