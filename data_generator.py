import sys
import rapidjson as json  # Ensure rapidjson is installed: pip install python-rapidjson
import uuid
import random

from dotenv import load_dotenv
from faker import Faker
from datetime import date, datetime

load_dotenv()
fake = Faker()

resorts = ["Vail", "Beaver Creek", "Breckenridge", "Keystone", "Crested Butte", "Park City", "Heavenly", "Northstar",
           "Kirkwood", "Whistler Blackcomb", "Perisher", "Falls Creek", "Hotham", "Stowe", "Mount Snow", "Okemo",
           "Hunter Mountain", "Mount Sunapee", "Attitash", "Wildcat", "Crotched", "Stevens Pass", "Liberty", "Roundtop",
           "Whitetail", "Jack Frost", "Big Boulder", "Alpine Valley", "Boston Mills", "Brandywine", "Mad River",
           "Hidden Valley", "Snow Creek", "Wilmot", "Afton Alps", "Mt. Brighton", "Paoli Peaks"]   

# Custom function to replace `none_or`
def maybe_none(value, probability=0.2):
    return value if random.random() > probability else None

def print_lift_ticket():
    global resorts, fake
    state = fake.state_abbr()
    lift_ticket = {
        'txid': str(uuid.uuid4()),
        'rfid': hex(random.getrandbits(96)),
        'resort': fake.random_element(elements=resorts),
        'purchase_time': datetime.utcnow().isoformat(),
        'expiration_time': date(2023, 6, 1).isoformat(),
        'days': fake.random_int(min=1, max=7),
        'name': fake.name(),
        'address': maybe_none({
            'street_address': fake.street_address(),
            'city': fake.city(),
            'state': state,
            'postalcode': fake.postcode_in_state(state)
        }),
        'phone': maybe_none(fake.phone_number()),
        'email': maybe_none(fake.email()),
        'emergency_contact': maybe_none({'name': fake.name(), 'phone': fake.phone_number()}),
    }
    d = json.dumps(lift_ticket) + '\n'
    sys.stdout.write(d)

if __name__ == "__main__":
    args = sys.argv[1:]
    total_count = int(args[0]) if args else 1  # Default to 1 if no argument is passed
    for _ in range(total_count):
        print_lift_ticket()
    print('')
