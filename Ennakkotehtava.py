import sqlite3
import csv
import json

#Luodaan tietokanta ja/tai muodostetaan yhteys siihen
try:
    connection = sqlite3.connect("harjoitus_sql.db")
    cursor = connection.cursor()
except:
    print("Error in creating or connecting to database")
else:
    print("Tietokanta \"harjoitus_sql\" luotu ja/tai yhdistetty")

#Luodaan taulut SQLiteen
create_table_1 = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT NOT NULL
);
"""
create_table_2 = """
CREATE TABLE IF NOT EXISTS products (
    sku TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    unit_price REAL NOT NULL,
    vat_code TEXT NOT NULL
);
"""
create_table_3 = """
CREATE TABLE IF NOT EXISTS stock_levels (
    sku TEXT NOT NULL,
    warehouse TEXT NOT NULL,
    qty_on_hand INTEGER NOT NULL,
    PRIMARY KEY (sku, warehouse)
);
"""
create_table_4 = """
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    order_date TEXT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
"""
create_table_5 = """
CREATE TABLE IF NOT EXISTS order_lines (
    order_id TEXT NOT NULL,
    sku TEXT NOT NULL,
    qty INTEGER NOT NULL,
    PRIMARY KEY (order_id, sku),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (sku) REFERENCES products(sku)
);
"""
try:
    cursor.execute(create_table_1)
    cursor.execute(create_table_2)
    cursor.execute(create_table_3)
    cursor.execute(create_table_4)
    cursor.execute(create_table_5)
except:
    print("Jotain vikaavikaavikaa taulujen luonnissa")
else:
    print("Taulut luotu tietokantaan")



#Luetaan csv-tiedostot ja viedään ne sqliteen
try:
    with open("customers.csv", 'r') as file:
        print("Asiakastiedot:")
        csv_lukija = csv.reader(file)
        next(csv_lukija)
        for row in csv_lukija:
            print(row)
            cursor.execute('INSERT INTO customers VALUES (?, ?)', row)
    with open("order_lines.csv", 'r') as file:
        print("Tilausrivit: ")
        csv_lukija = csv.reader(file)
        next(csv_lukija)
        for row in csv_lukija:
            print(row)
            cursor.execute('INSERT INTO order_lines VALUES (?, ?, ?)', row)
    with open("orders.csv", 'r') as file:
        print("Tilaukset: ")
        csv_lukija = csv.reader(file)
        next(csv_lukija)
        for row in csv_lukija:
            print(row)
            cursor.execute('INSERT INTO orders VALUES (?, ?, ?)', row)
    with open("products.csv", 'r') as file:
        print("Tuotteet: ")
        csv_lukija = csv.reader(file)
        next(csv_lukija)
        for row in csv_lukija:
            print(row)
            cursor.execute('INSERT INTO products VALUES (?, ?, ?, ?)', row)
    with open("stock_levels.csv", 'r') as file:
        print("Varastosaldot: ")
        csv_lukija = csv.reader(file)
        next(csv_lukija)
        for row in csv_lukija:
            print(row)
            cursor.execute('INSERT INTO stock_levels VALUES (?, ?, ?)', row)
except:
    print("Virhe tietojen tuomisessa tietokantaan")
else:
    print("Tiedot viety tietokantaan")


#Haetaan ALV-tiedot json-tiedostosta ja tarkennetaan sieltä hakupaikka
with open('tax_rules.json', 'r') as file:
    alv_tiedot = json.load(file)
    alvit = alv_tiedot["vat"]


#Lasketaan tilauksille nettosumma ja yhdistetään samalla tilausnumerolla olevat tilausrivit, järjestetään myös alvin mukaan)
try:
    laske_arvot = """
        SELECT order_lines.order_id,
        products.vat_code,
        SUM(order_lines.qty * products.unit_price) AS tilauksen_netto
        FROM order_lines
        JOIN products ON order_lines.sku = products.sku
        GROUP BY order_lines.order_id, products.vat_code
    ;
    """
    cursor.execute(laske_arvot)
    rivit = cursor.fetchall()
    
    #Tarkistetaan varastosaldot (verrataan tilattua määrää varaston määrään)
    tarkista_saldot = """
        SELECT order_lines.order_id,
        MIN(
            CASE WHEN stock_levels.qty_on_hand >= order_lines.qty
            THEN 1
            ELSE 0
            END
        ) AS loytyy
        FROM order_lines
        JOIN stock_levels ON stock_levels.sku = order_lines.sku
        GROUP BY order_lines.order_id
    ;
    """

    cursor.execute(tarkista_saldot)
    saldotilanne = cursor.fetchall()

    #Haetaan asiakkaiden nimet tietokannasta
    tarkista_nimi = """
        SELECT orders.order_id, customers.customer_name
        FROM customers
        JOIN orders ON customers.customer_id = orders.customer_id
    ;
    """
    cursor.execute(tarkista_nimi)
    nimihaku = cursor.fetchall()

    #Haetaan ja tallennetaan tietoja laskutoimituksia varten, viedään arvot varastomäärille ja nimille
    tilaukset = {}

    for order_id, vat_code, tilauksen_netto in rivit:
        alv_ryhma = alvit.get(vat_code, 0)
        alv_yhteensa = tilauksen_netto * alv_ryhma

        if order_id not in tilaukset:
            tilaukset[order_id] = {"netto": 0, "alv": 0, "maarat": "Tarkistamatta"}

        tilaukset[order_id]["netto"] += tilauksen_netto
        tilaukset[order_id]["alv"] += alv_yhteensa

    for order_id, loytyy in saldotilanne:
        if order_id not in tilaukset:
            tilaukset[order_id] = {"netto": 0, "alv": 0}
        tilaukset[order_id]["maarat"] = "Varastossa" if loytyy == 1 else "Ei varastossa"
    
    for order_id, customer_name in nimihaku:
        if order_id not in tilaukset:
            tilaukset[order_id] = {"netto": 0, "alv": 0, "maarat": "Tarkistamatta"}
        tilaukset[order_id]["nimi"] = customer_name
    
    #valmistellaan tulostusta varten säiliöt tarvittaville tiedoille
    data_otsikot = ['order_id', 'customer_name', 'net_total', 'vat_total', 'gross_total', 'is_fully_in_stock']
    data_rivit = []

    #käydään läpi tilaukset ja viimeistellään laskutoimitukset
    for order_id, tiedot in tilaukset.items():
        netto = tiedot.get("netto", 0)
        alv = tiedot.get("alv", 0)
        brutto = netto + alv
        maarat = tiedot.get("maarat", "Tietoa ei löydy")
        nimi = tiedot.get("nimi", "Tietoa ei löydy")
        print(f"Tilaus {order_id}: "f"Netto {netto:.2f} €, "f"ALV {alv:.2f} €, "f"Brutto {brutto:.2f} €, "f"Varastotilanne {maarat}, "f"Asiakkaan nimi {nimi}")

        #viedään tietoja tulostettavaksi
        data_rivit.append({'order_id': order_id, 'customer_name': nimi, 'net_total': f"{netto:.2f}", 'vat_total': f"{alv:.2f}", 'gross_total': f"{brutto:.2f}", 'is_fully_in_stock': maarat})
        
        #luodaan csv-tiedosto
        with open("order_totals.csv", "w", newline="", encoding="utf-8") as csvtiedosto:
            writer = csv.DictWriter(csvtiedosto, fieldnames=data_otsikot)
            writer.writeheader()
            writer.writerows(data_rivit)

except: 
    print("ei toiminut")
else:
    print("Jee, csv-tiedosto luotu!")

#katkaistaan yhteys
connection.commit()
if connection:
    connection.close()
    print("sqlite yhteys katkaistu")
