"""
FraudLens Payment Gateway
Real-time transaction processing engine
"""

from kafka import KafkaProducer
import json
import random
import time
import uuid
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

# ── Gateway Data ───────────────────────────────────────────────────

MERCHANT_CATEGORIES = {
    'upi':            0.28,
    'ecommerce':      0.20,
    'food_delivery':  0.13,
    'grocery':        0.10,
    'fuel':           0.07,
    'utilities':      0.06,
    'travel':         0.06,
    'entertainment':  0.04,
    'healthcare':     0.03,
    'jewellery':      0.03,
}

MERCHANTS = {
    'upi':            [('phonepe',    'PhonePe Pvt Ltd'),
                       ('googlepay',  'Google Pay India'),
                       ('paytm',      'One97 Communications'),
                       ('bhim',       'NPCI BHIM'),
                       ('amazonpay',  'Amazon Pay India')],
    'ecommerce':      [('flipkart',   'Flipkart Internet Pvt Ltd'),
                       ('amazon',     'Amazon Seller Services'),
                       ('myntra',     'Myntra Designs Pvt Ltd'),
                       ('meesho',     'Fashnear Technologies'),
                       ('nykaa',      'FSN E-Commerce Ventures')],
    'food_delivery':  [('swiggy',     'Bundl Technologies'),
                       ('zomato',     'Zomato Ltd'),
                       ('dominos',    'Jubilant FoodWorks'),
                       ('mcdonalds',  'Hardcastle Restaurants'),
                       ('ccd',        'Coffee Day Enterprises')],
    'grocery':        [('bigbasket',  'Supermarket Grocery Supplies'),
                       ('blinkit',    'Blink Commerce Pvt Ltd'),
                       ('jiomart',    'Reliance Retail Ltd'),
                       ('zepto',      'Kiranakart Technologies'),
                       ('dmart',      'Avenue Supermarts Ltd')],
    'fuel':           [('iocl',       'Indian Oil Corporation'),
                       ('hpcl',       'Hindustan Petroleum Corp'),
                       ('bpcl',       'Bharat Petroleum Corp'),
                       ('shell',      'Shell India Markets'),
                       ('essar',      'Essar Oil Ltd')],
    'utilities':      [('bescom',     'BESCOM Electricity'),
                       ('tatapower',  'Tata Power Company'),
                       ('jio',        'Reliance Jio Infocomm'),
                       ('airtel',     'Bharti Airtel Ltd'),
                       ('mseb',       'Maharashtra State Electricity')],
    'travel':         [('makemytrip', 'MakeMyTrip India Pvt Ltd'),
                       ('irctc',      'Indian Railway Catering'),
                       ('ola',        'ANI Technologies Pvt Ltd'),
                       ('uber',       'Uber India Systems'),
                       ('indigo',     'InterGlobe Aviation Ltd')],
    'entertainment':  [('bookmyshow', 'Bigtree Entertainment'),
                       ('hotstar',    'Novi Digital Entertainment'),
                       ('netflix',    'Netflix Entertainment'),
                       ('pvr',        'PVR Ltd'),
                       ('sonyliv',    'Sony Pictures Networks')],
    'healthcare':     [('apollo',     'Apollo Pharmacy Ltd'),
                       ('1mg',        'Tata 1mg Technologies'),
                       ('pharmeasy',  'API Holdings Pvt Ltd'),
                       ('practo',     'Practo Technologies'),
                       ('netmeds',    'Dadha Pharma Distribution')],
    'jewellery':      [('tanishq',    'Titan Company Ltd'),
                       ('malabar',    'Malabar Gold & Diamonds'),
                       ('kalyan',     'Kalyan Jewellers India'),
                       ('pcjeweller', 'PC Jeweller Ltd')],
}

AMOUNT_RANGES = {
    'upi':           (50,    45000),
    'ecommerce':     (299,   29999),
    'food_delivery': (80,    2400),
    'grocery':       (200,   5999),
    'fuel':          (500,   4999),
    'utilities':     (200,   11999),
    'travel':        (499,   24999),
    'entertainment': (99,    1999),
    'healthcare':    (150,   7999),
    'jewellery':     (4999,  199999),
}

INDIAN_CITIES = [
    ('MUM', 'Mumbai'),   ('DEL', 'Delhi'),     ('BLR', 'Bengaluru'),
    ('HYD', 'Hyderabad'), ('CHN', 'Chennai'),  ('KOL', 'Kolkata'),
    ('PUN', 'Pune'),     ('AMD', 'Ahmedabad'), ('JAI', 'Jaipur'),
    ('SRT', 'Surat'),    ('LKO', 'Lucknow'),   ('NGP', 'Nagpur'),
    ('IND', 'Indore'),   ('KCH', 'Kochi'),     ('CHD', 'Chandigarh'),
]

PAYMENT_METHODS = {
    'upi':          0.45,
    'debit_card':   0.25,
    'credit_card':  0.20,
    'net_banking':  0.07,
    'wallet':       0.03,
}

CARD_NETWORKS = ['Visa', 'Mastercard', 'RuPay']
ISSUING_BANKS = ['SBI', 'HDFC', 'ICICI',
                 'Axis', 'Kotak', 'Yes Bank', 'PNB', 'BOB']
FRAUD_LOCATIONS = [
    ('LOS', 'Lagos'),    ('MSC', 'Moscow'),  ('BCH', 'Bucharest'),
    ('KYV', 'Kyiv'),     ('JKT', 'Jakarta'), ('CAR', 'Caracas'),
    ('XXX', 'Unknown'),  ('MIN', 'Minsk'),
]
FRAUD_MERCHANTS = [
    ('intlwire',   'Intl Wire Transfer Services'),
    ('cryptootc',  'Digital Asset Exchange OTC'),
    ('offshore',   'Offshore Payments Processing'),
    ('forexremit', 'Foreign Remittance Gateway'),
    ('unknown',    'Unknown Merchant Entity'),
]

ACCOUNTS = [f"IN{str(i).zfill(8)}" for i in range(10000001, 10000501)]

# ── Helpers ────────────────────────────────────────────────────────


def weighted(d):
    return random.choices(list(d.keys()), weights=list(d.values()), k=1)[0]


def mask_account(account_id):
    return f"****{account_id[-4:]}"


def inr(amount):
    if amount >= 10000000:
        return f"INR {amount/10000000:.2f}Cr"
    if amount >= 100000:
        return f"INR {amount/100000:.2f}L"
    if amount >= 1000:
        return f"INR {amount/1000:.2f}K"
    return f"INR {amount:.2f}"


def generate():
    is_fraud = random.random() < 0.02
    now = datetime.now()
    category = weighted(MERCHANT_CATEGORIES)

    if is_fraud:
        amount = round(random.uniform(50000, 800000), 2)
        city_code, city = random.choice(FRAUD_LOCATIONS)
        hour = random.choice([0, 1, 2, 3, 4, 23])
        mid, merchant = random.choice(FRAUD_MERCHANTS)
        method = random.choice(['net_banking', 'credit_card'])
        category = random.choice(['upi', 'jewellery', 'ecommerce'])
        network = random.choice(CARD_NETWORKS)
        bank = random.choice(ISSUING_BANKS)
        risk_score = random.randint(78, 99)
    else:
        lo, hi = AMOUNT_RANGES[category]
        amount = round(random.uniform(lo, hi), 2)
        city_code, city = random.choice(INDIAN_CITIES)
        hour = random.randint(7, 23)
        mid, merchant = random.choice(MERCHANTS[category])
        method = weighted(PAYMENT_METHODS)
        network = random.choice(CARD_NETWORKS)
        bank = random.choice(ISSUING_BANKS)
        risk_score = random.randint(1, 25)

    account_id = random.choice(ACCOUNTS)
    txn_id = str(uuid.uuid4()).upper()

    return {
        "transaction_id":    txn_id,
        "account_id":        account_id,
        "merchant_id":       mid,
        "merchant_name":     merchant,
        "merchant_category": category,
        "amount_inr":        amount,
        "payment_method":    method,
        "card_network":      network,
        "issuing_bank":      bank,
        "city_code":         city_code,
        "city":              city,
        "hour_of_day":       hour,
        "day_of_week":       now.weekday(),
        "risk_score":        risk_score,
        "is_fraud":          int(is_fraud),
        "timestamp":         now.isoformat()
    }

# ── Display ────────────────────────────────────────────────────────


STATUS_MAP = {
    'captured':  'CAPTURED ',
    'flagged':   'FLAGGED  ',
    'review':    'REVIEW   ',
}


def main():
    print()
    print("  FraudLens Payment Gateway  |  Transaction Processing Engine  v2.0")
    print(
        f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}  |  Environment: PRODUCTION  |  Region: ap-south-1")
    print()
    print(f"  {'TIMESTAMP':<12}  {'TXN ID':<10}  {'STATUS':<10}  {'AMOUNT':>14}  {'MERCHANT':<30}  {'METHOD':<12}  {'RISK':>4}  {'CITY'}")
    print(
        f"  {'-'*12}  {'-'*10}  {'-'*10}  {'-'*14}  {'-'*30}  {'-'*12}  {'-'*4}  {'-'*12}")

    total = 0
    flagged = 0
    volume = 0.0

    while True:
        txn = generate()
        producer.send('transactions-raw', key=txn['account_id'], value=txn)

        total += 1
        volume += txn['amount_inr']

        ts = datetime.now().strftime('%H:%M:%S.%f')[:12]
        txn_short = txn['transaction_id'][:8]
        amt = inr(txn['amount_inr'])
        merch = txn['merchant_name'][:30]
        method = txn['payment_method'].replace('_', ' ').title()[:12]
        risk = txn['risk_score']
        city = txn['city']

        if txn['is_fraud']:
            flagged += 1
            status = 'FLAGGED'
            print(
                f"  {ts}  {txn_short}  {status:<10}  {amt:>14}  {merch:<30}  {method:<12}  {risk:>4}  {city}  <<< FRAUD ALERT")
        else:
            if total % 10 == 0:
                status = 'CAPTURED'
                print(
                    f"  {ts}  {txn_short}  {status:<10}  {amt:>14}  {merch:<30}  {method:<12}  {risk:>4}  {city}")

        if total % 500 == 0:
            print()
            print(
                f"  ── Gateway Stats ── {datetime.now().strftime('%H:%M:%S')} ─────────────────────────────────────────────")
            print(f"  Transactions Processed : {total:>8,}")
            print(f"  Total Volume           : {inr(volume):>14}")
            print(
                f"  Fraud Flagged          : {flagged:>8}  ({flagged/total*100:.2f}%)")
            print(f"  Avg Transaction Value  : {inr(volume/total):>14}")
            print(
                f"  ─────────────────────────────────────────────────────────────────────")
            print()
            print(f"  {'TIMESTAMP':<12}  {'TXN ID':<10}  {'STATUS':<10}  {'AMOUNT':>14}  {'MERCHANT':<30}  {'METHOD':<12}  {'RISK':>4}  {'CITY'}")
            print(
                f"  {'-'*12}  {'-'*10}  {'-'*10}  {'-'*14}  {'-'*30}  {'-'*12}  {'-'*4}  {'-'*12}")

        time.sleep(0.1)


if __name__ == '__main__':
    main()
