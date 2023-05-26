import psycopg2


def create_db(cursor):
    cursor.execute("""
        DROP TABLE Clients, Phones, Clients_Phones CASCADE;
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Clients(
        clients_id SERIAL PRIMARY KEY,
        first_name VARCHAR(40) NOT NULL,
        last_name VARCHAR(40) NOT NULL,
        email VARCHAR(80) NOT NULL UNIQUE
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Phones(
        phones_id SERIAL PRIMARY KEY,
        phones_number BIGINT UNIQUE
    );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Clients_Phones(
        clients_id INTEGER REFERENCES clients(clients_id),
        phones_id INTEGER REFERENCES phones(phones_id)
    );
    """)
    conn.commit()


def add_client(cursor, first_name, last_name, email, phones_number=None):
    cursor.execute("""
    INSERT INTO Clients(first_name, last_name, email)
        VALUES(%s, %s, %s);
    """, (first_name, last_name, email))

    if phones_number is not None:
        cursor.execute("""
        INSERT INTO Phones(phones_number)
            VALUES(%s);
        """, (phones_number,))

        cursor.execute("""
        INSERT INTO Clients_Phones(clients_id, phones_id)
            VALUES((SELECT clients_id FROM Clients WHERE email=%s),
            (SELECT  phones_id FROM Phones WHERE phones_number=%s)) ;
        """, (email, phones_number))

    conn.commit()


def add_phone(cursor, client_id, phones_number):
    cursor.execute("""
    INSERT INTO Phones(phones_number)
        VALUES(%s);
    """, (phones_number,))

    cursor.execute("""
    INSERT INTO Clients_Phones(clients_id, phones_id)
        VALUES(%s,
        (SELECT  phones_id FROM Phones WHERE phones_number=%s)) ;
    """, (client_id, phones_number))

    conn.commit()


def change_client(cursor, client_id, first_name=None, last_name=None, email=None, phones=None):
    if first_name is not None:
        cursor.execute("""
        UPDATE Clients
        SET first_name=%s
        WHERE clients_id=%s;
        """, (first_name, client_id))

    if last_name is not None:
        cursor.execute("""
        UPDATE Clients
        SET last_name=%s
        WHERE clients_id=%s;
        """, (last_name, client_id))

    if email is not None:
        cursor.execute("""
        UPDATE Clients
        SET email=%s
        WHERE clients_id=%s;
        """, (email, client_id))

    if phones is not None:
        cursor.execute("""
        UPDATE Phones
        SET phones_number=%s
        WHERE phones_id=(SELECT phones_id FROM Clients_Phones WHERE clients_id=$s ;
        """, (phones,))


def delete_phone(cursor, client_id, phones_number):
    cursor.execute("""
    DELETE FROM Clients_Phones
        WHERE clients_id=%s AND phones_id=(SELECT phones_id FROM Phones WHERE phones_number=%s);
    """, (client_id, phones_number))

    cursor.execute("""
    DELETE FROM Phones
        WHERE phones_number=%s;
    """, (phones_number,))

    conn.commit()


def delete_client(cursor, client_id):
    # cursor.execute("""
    # DELETE FROM Phones
    #     WHERE phones_id=(SELECT phones_id FROM WHERE clients_id=%s;
    # """, (client_id, ))

    cursor.execute("""
    DELETE FROM Clients_Phones
    WHERE clients_id=%s;
    """, (client_id, ))

    cursor.execute("""
    DELETE FROM Clients
        WHERE clients_id=%s;
    """, (client_id,))

    conn.commit()

def find_client(cursor, **kwargs):
    set_lst = ', '.join(f"{k}=%s" for k in kwargs)
    query = 'SELECT first_name, last_name, email, phones_number FROM Clients ' \
            'LEFT JOIN clients_phones USING(clients_id) ' \
            'LEFT JOIN phones USING(phones_id) WHERE ' + set_lst

    cursor.execute(query, (*kwargs.values(),))
    return cur.fetchone()


with psycopg2.connect(database="homework_db", user="postgres", password="Tremkazan/ruRafik") as conn:
    with conn.cursor() as cur:


        db = create_db(cur)

        new_client = add_client(cur,'Илья', 'Кириллов', 'rollysuper', 89876543210)
        cur.execute("""
                SELECT first_name, last_name, email, phones_number FROM Clients
                LEFT JOIN clients_phones USING(clients_id)
                LEFT JOIN phones USING(phones_id);
                """)
        print(cur.fetchone())

        new_phone = add_phone(cur, 1, 89870123456)
        cur.execute("""
        SELECT first_name, last_name, email, phones_number FROM Clients
        LEFT JOIN clients_phones USING(clients_id)
        LEFT JOIN phones USING(phones_id);
        """)
        print(cur.fetchall())

        change = change_client(cur, 1, first_name='Ваня')
        cur.execute("""
        SELECT first_name, last_name, email, phones_number FROM Clients
        LEFT JOIN clients_phones USING(clients_id)
        LEFT JOIN phones USING(phones_id);
        """)
        print(cur.fetchall())

        delete_number = delete_phone(cur, 1, 89876543210)
        cur.execute("""
        SELECT first_name, last_name, email, phones_number FROM Clients
        LEFT JOIN clients_phones USING(clients_id)
        LEFT JOIN phones USING(phones_id);
        """)
        print(cur.fetchall())

        find = find_client(cur, email='rollysuper')
        print(find)

        delete_cliento =  delete_client(cur, 1)
        cur.execute("""
        SELECT first_name, last_name, email, phones_number FROM Clients
        LEFT JOIN clients_phones USING(clients_id)
        LEFT JOIN phones USING(phones_id);
        """)
        print(cur.fetchall())



conn.close