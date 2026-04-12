import os, django, json
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "estate_admin.settings")
django.setup()
 
from django.db import connection
 
with connection.cursor() as c:
    c.execute("SELECT id, images FROM listings WHERE images IS NOT NULL LIMIT 5")
    rows = c.fetchall()
    for row in rows:
        print(f"ID: {row[0]}")
        print(f"images type: {type(row[1])}")
        print(f"images value: {json.dumps(row[1], indent=2)[:300]}")
        print("---")