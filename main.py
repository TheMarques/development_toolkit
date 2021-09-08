from core.database.sql_lite3 import Database


if __name__ == '__main__':
    
    print("This is a shop program.")

    db = Database(name="shop", run_template=True)

    print("Customer List.")
    for customer in db.all("customer"):
        print(customer)

    print("Items List.")
    for item in db.all("item"):
        print(item)

    # Add a new entry
    # new_client = {"name": "Matheus", "address": "Slovakia Time Square"}
    # new_item = {"description": "bottle", "price": 0.25}
    # db.insert("customer", new_client)
    # db.insert("item", new_item)

    # Update an existing value
    # #db.update("item", {"price": 0.15}, {"description": "bottle"})

    # Delete an existing value
    # db.delete("customer", {"id": 7})
    # db.update("customer", {"address":"Rua de Portugal , lote 23"}, {"name":"Ariana"})


