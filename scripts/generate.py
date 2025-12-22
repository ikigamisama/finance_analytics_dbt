import pandas as pd
import numpy as np
import pandas_datareader.data as web

from synthetic_data_crafter import SyntheticDataCrafter
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine

import random
np.random.seed(42)


class FinancialDataGenerator:
    def __init__(self, start_date, num_customers=10000):
        self.us_state = pd.read_csv('./data/us.csv')
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime('today')
        self.num_customers = num_customers

        # Core entities
        self.customers = []
        self.accounts = []
        self.transactions = []
        self.products = []
        self.merchants = []
        self.customer_interactions = []
        self.credit_applications = []
        self.fraud_alerts = []
        self.marketing_campaigns = []
        self.loan_payments = []
        self.economic_indicators = []
        self.branch_locations = []
        self.atm_locations = []
        self.risk_assessments = []
        self.account_events = []
        self.regulatory_reports = []
        self.customer_segments_history = []

    def generate_products(self):
        product_types = [
            {'product_id': 1, 'product_name': 'Checking Account', 'category': 'Deposit',
             'interest_rate': 0.01, 'min_balance': 0, 'monthly_fee': 0, 'overdraft_limit': 100,
             'product_tier': 'Basic', 'is_premium': False},
            {'product_id': 2, 'product_name': 'Savings Account', 'category': 'Deposit',
             'interest_rate': 0.03, 'min_balance': 100, 'monthly_fee': 0, 'overdraft_limit': 0,
             'product_tier': 'Basic', 'is_premium': False},
            {'product_id': 3, 'product_name': 'Credit Card', 'category': 'Credit',
             'interest_rate': 0.18, 'min_balance': 0, 'monthly_fee': 0, 'overdraft_limit': 0,
             'product_tier': 'Standard', 'is_premium': False},
            {'product_id': 4, 'product_name': 'Personal Loan', 'category': 'Loan',
             'interest_rate': 0.08, 'min_balance': 0, 'monthly_fee': 0, 'overdraft_limit': 0,
             'product_tier': 'Standard', 'is_premium': False},
            {'product_id': 5, 'product_name': 'Mortgage', 'category': 'Loan',
             'interest_rate': 0.045, 'min_balance': 0, 'monthly_fee': 0, 'overdraft_limit': 0,
             'product_tier': 'Premium', 'is_premium': True},
            {'product_id': 6, 'product_name': 'Investment Account', 'category': 'Investment',
             'interest_rate': 0.0, 'min_balance': 1000, 'monthly_fee': 25, 'overdraft_limit': 0,
             'product_tier': 'Premium', 'is_premium': True},
            {'product_id': 7, 'product_name': 'Business Checking', 'category': 'Deposit',
             'interest_rate': 0.015, 'min_balance': 1000, 'monthly_fee': 15, 'overdraft_limit': 500,
             'product_tier': 'Business', 'is_premium': True},
            {'product_id': 8, 'product_name': 'Auto Loan', 'category': 'Loan',
             'interest_rate': 0.055, 'min_balance': 0, 'monthly_fee': 0, 'overdraft_limit': 0,
             'product_tier': 'Standard', 'is_premium': False},
            {'product_id': 9, 'product_name': 'Premium Credit Card', 'category': 'Credit',
             'interest_rate': 0.15, 'min_balance': 0, 'monthly_fee': 95, 'overdraft_limit': 0,
             'product_tier': 'Premium', 'is_premium': True},
            {'product_id': 10, 'product_name': 'Money Market Account', 'category': 'Deposit',
             'interest_rate': 0.04, 'min_balance': 2500, 'monthly_fee': 0, 'overdraft_limit': 0,
             'product_tier': 'Premium', 'is_premium': True},
        ]
        self.products = pd.DataFrame(product_types)
        return self.products

    def generate_merchants(self, num_merchants=15000):
        schema_merchants = [
            {"label": "_location", "key_label": "lambda", "group": "advanced",
                "options": {"func": lambda: self.us_state.sample(n=1).iloc[0]}},
            {"label": "merchant_id", "key_label": "row_number",
                "group": 'basic', "options": {"blank_percentage": 0}},
            {"label": "merchant_name", "key_label": "fake_company_name",
                "group": 'personal', "options": {"blank_percentage": 0}},
            {"label": "category", "key_label": "custom_list", "group": 'basic', "options": {
                "custom_format": "Grocery,Restaurant,Gas Station,Retail,Entertainment,Healthcare,Utilities,Travel,Online Shopping,Services"}},
            {"label": "mcc_code", "key_label": "number",
                "group": "basic", "options": {'min': 1000, 'max': 9999}},
            {"label": "city", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda row: row['_location']['city']}},
            {"label": "state", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda row: row['_location']['state_id']}},
            {"label": "country", "key_label": "lambda",
                "group": "advanced", "options": {'func': lambda: "USA"}},
            {"label": "latitude", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda row: row['_location']['lat']}},
            {"label": "longitude", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda row: row['_location']['lng']}},
            {"label": "risk_rating", "key_label": "custom_list", "group": 'basic',
                "options": {"blank_percentage": 0, "custom_format": "Low,Medium,High"}},
            {"label": "avg_transaction_amount", "key_label": "number",
                "group": "basic", "options": {'decimals': 2}},
            {"label": "is_online", "key_label": "boolean", "group": "basic"},
            {
                "label": "established_date",
                "key_label": "datetime",
                "group": "basic",
                "options": {
                    "from_date": "01/01/2000",
                    "to_date": self.end_date.strftime('%m/%d/%Y'),
                    "date_format": 'yyyy-mm-dd'
                }
            },
        ]
        self.merchants = pd.DataFrame(
            SyntheticDataCrafter(schema_merchants).many(random.randint(num_merchants, (num_merchants * 2))).data)
        self.merchants['established_date'] = pd.to_datetime(
            self.merchants['established_date'])
        self.merchants = self.merchants.drop(columns=['_location'])
        return self.merchants

    def _age_from_mdy(self, date_str, reference_date=None):
        birth_date = datetime.strptime(date_str, "%m/%d/%Y").date()
        today = reference_date or date.today()
        age = today.year - birth_date.year

        if (today.month, today.day) < (birth_date.month, birth_date.day):
            age -= 1

        return age

    def generate_customers(self):
        from_date = pd.to_datetime('today') - pd.DateOffset(years=70)
        to_date = pd.to_datetime('today') - pd.DateOffset(years=18)
        signup_date = self.start_date + timedelta(days=random.randint(0, 730))
        delta_days = (pd.to_datetime('today') - self.start_date).days

        schema_customers = [
            {"label": "_location", "key_label": "lambda", "group": "advanced",
                "options": {"func": lambda: self.us_state.sample(n=1).iloc[0]}},
            {"label": "customer_id", "key_label": "row_number", "group": 'basic'},
            {"label": "first_name", "key_label": "first_name", "group": "personal"},
            {"label": "last_name", "key_label": "last_name", "group": "personal"},
            {"label": "email", "key_label": "email_address", "group": "it"},
            {"label": "phone", "key_label": "phone", "group": "location"},
            {
                "label": "date_of_birth",
                "key_label": "datetime",
                "group": "basic",
                "options": {
                    "from_date": from_date.strftime('%m/%d/%Y'),
                    "to_date": to_date.strftime('%m/%d/%Y')
                }
            },
            {"label": "age", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda x: self._age_from_mdy(x['date_of_birth'])}},
            {"label": "ssn", "key_label": "ssn", "group": "personal"},
            {"label": "address", "key_label": "street_address", "group": "location"},
            {"label": "city", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda row: row['_location']['city']}},
            {"label": "state", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda row: row['_location']['state_id']}},
            {"label": "zip_code", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda row: row['_location']['zip']}},
            {"label": "country", "key_label": "lambda",
                "group": "advanced", "options": {'func': lambda: "USA"}},
            {"label": "signup_date", "key_label": "lambda", "group": "advanced", "options": {
                'func': lambda: self.start_date + timedelta(days=random.randint(0, delta_days))}},
            {"label": "credit_score", "key_label": "number",
                "group": "basic", "options": {'min': 300, 'max': 800}},
            {"label": "annual_income", "key_label": "number",
                "group": "basic", "options": {'min': 20000, 'max': 500000}},
            {
                "label": "employment_status",
                "key_label": "custom_list",
                "group": 'basic',
                "options": {"blank_percentage": 0, "custom_format": "Employed,Self-Employed,Retired,Student,Unemployed"}
            },
            {"label": "employer", "key_label": "fake_company_name",
                "group": 'personal', "options": {"blank_percentage": 0.2}},
            {"label": "job_title", "key_label": "job_title",
                "group": 'personal', "options": {"blank_percentage": 0.2}},
            {"label": "education_level", "key_label": "education_level",
                "group": 'personal', "options": {"blank_percentage": 0}},
            {"label": "marital_status", "key_label": "marital_status",
                "group": 'personal', "options": {"blank_percentage": 0}},
            {"label": "number_of_dependents", "key_label": "number",
                "group": 'basic', "options": {'min': 0, 'max': 5}},
            {"label": "home_ownership", "key_label": "custom_list", "group": 'basic',
                "options": {"blank_percentage": 0, "custom_format": "Own,Rent,Mortgage"}},
            {"label": "customer_segment", "key_label": "custom_list", "group": 'basic',
                "options": {"custom_format": "Mass Market,Affluent,Premium,Business"}},
            {"label": "life_stage", "key_label": "custom_list", "group": 'basic', "options": {
                "custom_format": "Young Professional,Family,Empty Nester,Retiree,Student"}},
            {"label": "risk_segment", "key_label": "custom_list",
                "group": 'basic', "options": {"custom_format": "Low,Medium,High"}},
            {"label": "is_active", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda: random.choice([True, True, True, False])}},
            {"label": "preferred_channel", "key_label": "custom_list", "group": 'basic',
                "options": {"custom_format": "Online,Mobile,Branch,Phone"}},
            {"label": "marketing_opt_in", "key_label": "boolean", "group": "basic"},
            {"label": "loyalty_tier", "key_label": "custom_list", "group": 'basic',
                "options": {"custom_format": "Bronze,Silver,Gold,Platinum"}},
            {"label": "customer_lifetime_value", "key_label": "number",
                "group": "basic", "options": {'min': 1000, 'max': 100000}},
            {"label": "churn_risk_score", "key_label": "number",
                "group": "basic", "options": {'min': 0, 'max': 1, 'decimals': 2}},
            {
                "label": "last_login_date",
                "key_label": "datetime",
                "group": "basic",
                "options": {
                    "blank_percentage": 0.1,
                    "from_date": signup_date.strftime('%m/%d/%Y'),
                    "to_date": self.end_date.strftime('%m/%d/%Y'),
                    "date_format": 'yyyy-mm-dd'
                }
            },
            {"label": "acquisition_channel", "key_label": "custom_list", "group": 'basic',
                "options": {"custom_format": "Online,Branch,Referral,Partner,Marketing Campaign"}},
        ]

        self.customers = pd.DataFrame(SyntheticDataCrafter(
            schema_customers).many(self.num_customers).data)
        self.customers = self.customers.drop(columns=['_location'])
        return self.customers

    def generate_accounts(self):
        account_id = 1
        for _, customer in self.customers.iterrows():
            num_accounts = random.randint(1, 5)
            for _ in range(num_accounts):
                product = self.products.sample(1).iloc[0]
                open_date = customer['signup_date'] + \
                    timedelta(days=random.randint(0, 365))

                if product['category'] == 'Deposit':
                    balance = round(random.uniform(100, 100000), 2)
                elif product['category'] == 'Credit':
                    credit_limit = random.uniform(1000, 50000)
                    balance = round(-random.uniform(0, credit_limit * 0.7), 2)
                elif product['category'] == 'Loan':
                    balance = round(-random.uniform(5000, 500000), 2)
                else:
                    balance = round(random.uniform(1000, 500000), 2)

                account_status = random.choice(
                    ['Active', 'Active', 'Active', 'Dormant', 'Closed'])

                close_date = None
                if account_status == 'Closed' or random.random() < 0.1:
                    close_date = open_date + \
                        timedelta(days=random.randint(30, 1000))

                self.accounts.append({
                    'account_id': account_id,
                    'customer_id': customer['customer_id'],
                    'product_id': product['product_id'],
                    'account_number': str(random.randint(10**11, 10**15)),
                    'account_status': account_status,
                    'open_date': open_date,
                    'close_date': close_date,
                    'current_balance': balance,
                    'available_balance': round(
                        balance * random.uniform(0.8, 1.0), 2
                    ) if balance > 0 else balance,
                    'credit_limit': round(random.uniform(1000, 50000), 2) if product['category'] == 'Credit' else None,
                    'currency': 'USD',
                    'interest_rate': round(product['interest_rate'] * random.uniform(0.9, 1.1), 4),
                    'minimum_payment': round(abs(balance) * 0.02, 2) if product['category'] == 'Credit' else None,
                    'payment_due_date': (
                        datetime.now().date() + timedelta(days=random.randint(1, 30))
                    ) if product['category'] in ['Credit', 'Loan'] else None,
                    'last_statement_date': open_date + timedelta(days=random.randint(0, max(1, (self.end_date - open_date).days))),
                    'autopay_enabled': random.choice([True, False]),
                    'overdraft_protection': random.choice([True, False]) if product['category'] == 'Deposit' else False,
                    'primary_account': random.choice([True, False]),
                })

                account_id += 1

        self.accounts = pd.DataFrame(self.accounts)
        return self.accounts

    def _get_amount_by_trans(self, trans_type):
        if trans_type in ['Purchase', 'ATM Withdrawal', 'Fee']:
            amount = round(-random.uniform(5, 2000), 2)
        elif trans_type in ['Deposit', 'Refund']:
            amount = round(random.uniform(50, 5000), 2)
        else:
            amount = round(random.choice([1, -1])
                           * random.uniform(10, 3000), 2)

        return amount

    def generate_transactions(self, num_transactions=100000):
        active_accounts = self.accounts[self.accounts['account_status'] == 'Active']

        schema_transactions = [
            {"label": "_account", "key_label": "lambda", "group": "advanced",
                "options": {"func": lambda: active_accounts.sample(1).iloc[0]}},
            {"label": "_merchant", "key_label": "lambda", "group": "advanced",
                "options": {"func": lambda: self.merchants.sample(1).iloc[0]}},
            {"label": "transaction_id", "key_label": "row_number", "group": 'basic'},
            {"label": "account_id", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda x: x['_account']['account_id']}},
            {"label": "customer_id", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda x: x['_account']['customer_id']}},
            {"label": "merchant_id", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda x: x['_merchant']['merchant_id']}},
            {
                "label": "transaction_date",
                "key_label": "datetime",
                "group": "basic",
                "options": {
                    "from_date": active_accounts.sample(1).iloc[0]['open_date'].strftime('%m/%d/%Y'),
                    "to_date": self.end_date.strftime('%m/%d/%Y'),
                    "date_format": 'yyyy-mm-dd'
                }
            },
            {"label": "transaction_type", "key_label": "custom_list", "group": 'basic', "options": {
                "custom_format": "Purchase,ATM Withdrawal,Transfer,Payment,Deposit,Refund,Fee"}},
            {"label": "amount", "key_label": "lambda", "group": "advanced", "options": {
                'func': lambda x: self._get_amount_by_trans(x['transaction_type'])}},
            {"label": "currency", "key_label": "lambda",
                "group": "advanced", "options": {'func': lambda: "USD"}},
            {"label": "channel", "key_label": "custom_list", "group": 'basic',
                "options": {"custom_format": "Online,Mobile,ATM,Branch,POS"}},
            {"label": "merchant_category", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda x: x['_merchant']['category']}},
            {"label": "mcc_code", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda x: x['_merchant']['mcc_code']}},
            {"label": "description", "key_label": "lambda", "group": "advanced", "options": {
                'func': lambda x: f"{x['transaction_type']} at {x['_merchant']['merchant_name']}"}},
            {"label": "is_fraud", "key_label": "lambda", "group": "advanced", "options": {
                'func': lambda: True if random.random() < 0.001 else False}},
            {"label": "fraud_score", "key_label": "lambda", "group": "advanced", "options": {
                'func': lambda x: random.uniform(0, 1) if x['is_fraud'] else random.uniform(0, 0.3)}},
            {"label": "location_city", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda x: x['_merchant']['city']}},
            {"label": "location_state", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda x: x['_merchant']['state']}},
            {"label": "location_country", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda x: x['_merchant']['country']}},
            {"label": "latitude", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda x: x['_merchant']['latitude']}},
            {"label": "longitude", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda x: x['_merchant']['longitude']}},
            {"label": "device_id", "key_label": "uuid_v4",
                "group": "it", "options": {'blank_percentage': 0.3}},
            {"label": "ip_address", "key_label": "ip_address_v4",
                "group": "it", "options": {'blank_percentage': 0.2}},
            {"label": "is_international", "key_label": "boolean", "group": "basic"},
            {"label": "authorization_code", "key_label": "character_sequence",
                "group": "advanced", "options": {'format': "^^######"}},
            {"label": "card_last_four", "key_label": "lambda", "group": "advanced", "options": {
                'func': lambda x: random.randint(0, 9999) if x['transaction_type'] in ['Purchase', 'ATM Withdrawal'] else None}},
            {"label": "is_recurring", "key_label": "lambda", "group": "advanced", "options": {
                'func': lambda x: random.choice([True, False]) if x['transaction_type'] == 'Purchase' else False}},
            {"label": "hour_of_day", "key_label": "number",
                "group": "basic", "options": {'min': 0, 'max': 23}},
            {"label": "day_of_week", "key_label": "number",
                "group": "basic", "options": {'min': 0, 'max': 6}},
            {"label": "is_weekend", "key_label": "lambda", "group": "advanced", "options": {
                'func': lambda x: True if x['day_of_week'] >= 5 else False}},
            {"label": "distance_from_home_km", "key_label": "number",
                "group": "basic", "options": {'min': 0, 'max': 500, 'decimals': 2}},
            {"label": "merchant_risk_score", "key_label": "number",
                "group": "basic", "options": {'min': 0, 'max': 1, 'decimals': 2}},
            {"label": "velocity_24h", "key_label": "number",
                "group": "basic", "options": {'min': 1, 'max': 10}},
            {"label": "amount_deviation_score", "key_label": "number",
                "group": "basic", "options": {'min': 0, 'max': 1, 'decimals': 2}},
            {"label": "processing_time_ms", "key_label": "number",
                "group": "basic", "options": {'min': 100, 'max': 5000}},
            {"label": "decline_reason", "key_label": "lambda", "group": "advanced", "options": {
                'func': lambda: random.choice([None, None, None, 'Insufficient Funds', 'Invalid Card', 'Fraud Suspected'])}},
        ]

        self.transactions = pd.DataFrame(SyntheticDataCrafter(
            schema_transactions).many(random.randint(num_transactions, (num_transactions * 2))).data)
        self.transactions = self.transactions.drop(
            columns=['_account', '_merchant'])
        self.transactions['transaction_date'] = pd.to_datetime(
            self.transactions['transaction_date'])
        self.transactions = self.transactions.sort_values(
            'transaction_date').reset_index(drop=True)
        return self.transactions

    def _get_decision_output(self, credit_score):
        if credit_score >= 720:
            decision = 'Approved'
            approval_prob = 0.9
        elif credit_score >= 650:
            decision = random.choice(
                ['Approved', 'Approved', 'Pending', 'Declined'])
            approval_prob = 0.6
        else:
            decision = random.choice(['Declined', 'Declined', 'Pending'])
            approval_prob = 0.2

        return decision, approval_prob

    def generate_credit_applications(self, num_applications=10000):
        schema_credit_applications = [
            {"label": "_customer", "key_label": "lambda", "group": "advanced",
                "options": {"func": lambda: self.customers.sample(1).iloc[0]}},
            {"label": "application_id", "key_label": "row_number", "group": 'basic'},
            {"label": "customer_id", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda x: x['_customer']['customer_id']}},
            {"label": "product_id", "key_label": "number",
                "group": "basic", "options": {'min': 3, 'max': 5}},
            {
                "label": "application_date",
                "key_label": "datetime",
                "group": "basic",
                "options": {
                    "from_date": self.customers.sample(1).iloc[0]['signup_date'].strftime('%m/%d/%Y'),
                    "to_date": self.end_date.strftime('%m/%d/%Y'),
                    "date_format": 'yyyy-mm-dd'
                }
            },
            {"label": "requested_amount", "key_label": "number",
                "group": "basic", "options": {'min': 1000, 'max': 100000}},
            {"label": "requested_term_months", "key_label": "custom_list",
                "group": 'basic', "options": {"custom_format": "12,24,36,48,60,120,360"}},
            {"label": "credit_score_at_application", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda x: x['_customer']['credit_score']}},
            {"label": "annual_income", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda x: x['_customer']['annual_income']}},
            {"label": "debt_to_income_ratio", "key_label": "number",
                "group": "basic", "options": {'min': 0.1, 'max': 0.6, 'decimals': 1}},
            {"label": "employment_length_years", "key_label": "number",
                "group": "basic", "options": {'min': 0, 'max': 30}},
            {"label": "decision", "key_label": "lambda", "group": "advanced", "options": {
                'func': lambda x: self._get_decision_output(int(x['_customer']['credit_score']))[0]}},
            {"label": "decision_date", "key_label": "lambda", "group": "advanced", "options": {
                'func': lambda x: pd.to_datetime(x['application_date']) + pd.Timedelta(days=random.randint(1, 14))}},
            {"label": "approved_amount", "key_label": "lambda", "group": "advanced", "options": {
                'func': lambda x: round(random.uniform(1000, 100000), 2) if x['decision'] == 'Approved' else None}},
            {"label": "approved_rate", "key_label": "lambda", "group": "advanced", "options": {
                'func': lambda x: round(random.uniform(0.04, 0.20), 2) if x['decision'] == 'Approved' else None}},
            {"label": "application_channel", "key_label": "custom_list", "group": 'basic',
                "options": {"custom_format": "Online,Branch,Phone,Mobile"}},
            {"label": "approval_probability_score", "key_label": "lambda", "group": 'advanced', "options": {
                'func': lambda x: self._get_decision_output(int(x['_customer']['credit_score']))[1]}},
            {"label": "risk_grade", "key_label": "custom_list",
                "group": 'basic', "options": {"custom_format": "A,B,C,D,E,F,G"}},
        ]

        self.credit_applications = pd.DataFrame(SyntheticDataCrafter(
            schema_credit_applications).many(random.randint(num_applications, (num_applications * 2))).data)
        self.credit_applications = self.credit_applications.drop(columns=[
                                                                 '_customer'])
        return self.credit_applications

    def generate_fraud_alerts(self):
        fraud_transactions = self.transactions[self.transactions['is_fraud'] == True]

        for idx, trans in fraud_transactions.iterrows():
            alert_date = pd.to_datetime(trans['transaction_date']) + \
                timedelta(minutes=random.randint(1, 120))

            schema_fraud_alerts = [
                {"label": "alert_id", "key_label": "lambda",
                    "group": "advanced", "options": {'func': lambda: idx}},
                {"label": "transaction_id", "key_label": "lambda", "group": "advanced",
                    "options": {'func': lambda: trans['transaction_id']}},
                {"label": "customer_id", "key_label": "lambda", "group": "advanced",
                    "options": {'func': lambda: trans['customer_id']}},
                {"label": "account_id", "key_label": "lambda", "group": "advanced",
                    "options": {'func': lambda: trans['account_id']}},
                {"label": "alert_date", "key_label": "lambda",
                    "group": "advanced", "options": {'func': lambda: alert_date}},
                {"label": "alert_type", "key_label": "custom_list", "group": 'basic', "options": {
                    "custom_format": "Unusual Spending,Geographic Anomaly,Velocity Check,High Risk Merchant"}},
                {"label": "alert_severity", "key_label": "custom_list", "group": 'basic',
                    "options": {"custom_format": "Low,Medium,High,Critical"}},
                {"label": "investigation_status", "key_label": "custom_list", "group": 'basic', "options": {
                    "custom_format": "Open,Under Review,Resolved - Fraud,Resolved - Legitimate,False Positive"}},
                {"label": "resolution_date", "key_label": "lambda", "group": "advanced", "options": {
                    'func': lambda x: x['alert_date'] + timedelta(days=random.randint(1, 30)) if random.random() > 0.3 else None}},
                {"label": "amount_recovered", "key_label": "lambda", "group": "advanced", "options": {
                    'func': lambda: trans['amount'] * random.uniform(0, 1) if random.random() > 0.5 else 0}},
                {"label": "assigned_to", "key_label": "full_name", "group": "personal"},
                {"label": "notes", "key_label": "lambda", "group": "advanced", "options": {
                    'func': lambda: f"Suspicious activity detected: {trans['description']}"}},
            ]
            self.fraud_alerts.append(
                SyntheticDataCrafter(schema_fraud_alerts).one())

        self.fraud_alerts = pd.DataFrame(self.fraud_alerts)
        return self.fraud_alerts

    def _generate_notes(self, sentiment, reason):
        if sentiment < -0.3:
            notes = f"Customer contacted support regarding {reason.lower()} and was dissatisfied."
        elif sentiment > 0.3:
            notes = f"Customer contacted support regarding {reason.lower()} and was satisfied."
        else:
            notes = f"Customer contacted support regarding {reason.lower()}."

        return notes

    def generate_customer_interactions(self, num_interactions=100000):
        schema_customer_interactions = [
            {"label": "_customer", "key_label": "lambda", "group": "advanced",
                "options": {"func": lambda: self.customers.sample(1).iloc[0]}},
            {"label": "interaction_id", "key_label": "row_number", "group": 'basic'},
            {"label": "customer_id", "key_label": "lambda", "group": "advanced",
                "options": {"func": lambda x: x['_customer']['customer_id']}},
            {
                "label": "interaction_date",
                "key_label": "datetime",
                "group": "basic",
                "options": {
                    "from_date": self.customers.sample(1).iloc[0]['signup_date'].strftime('%m/%d/%Y'),
                    "to_date": self.end_date.strftime('%m/%d/%Y'),
                    "date_format": 'yyyy-mm-dd'
                }
            },
            {"label": "interaction_type", "key_label": "custom_list", "group": 'basic', "options": {
                "custom_format": "Phone Call,Email,Chat,Branch Visit,Social Media"}},
            {"label": "reason", "key_label": "custom_list", "group": 'basic', "options": {
                "custom_format": "Account Inquiry,Transaction Dispute,Product Information,Technical Support,Complaint,Fraud Report,Service Request"}},
            {"label": "duration_minutes", "key_label": "number",
                "group": "basic", "options": {'min': 2, 'max': 120}},
            {"label": "sentiment_score", "key_label": "number",
                "group": "basic", "options": {'min': -1, 'max': 1, 'decimals': 2}},
            {"label": "satisfaction_rating", "key_label": "lambda", "group": "advanced", "options": {
                "func": lambda: random.randint(1, 5) if random.random() > 0.3 else None}},
            {"label": "resolved", "key_label": "boolean", "group": "basic"},
            {"label": "escalated", "key_label": "lambda", "group": "advanced", "options": {
                "func": lambda: random.choice([True, False]) if random.random() < 0.1 else False}},
            {"label": "agent_id", "key_label": "uuid_v4", "group": "it"},
            {"label": "notes", "key_label": "lambda", "group": "advanced", "options": {
                "func": lambda x: self._generate_notes(x['sentiment_score'], x['reason'])}},
        ]

        self.customer_interactions = pd.DataFrame(SyntheticDataCrafter(
            schema_customer_interactions).many(random.randint(num_interactions, (num_interactions * 2))).data)
        self.customer_interactions['interaction_date'] = pd.to_datetime(
            self.customer_interactions['interaction_date'])
        self.customer_interactions = self.customer_interactions.drop(columns=[
                                                                     '_customer'])
        return self.customer_interactions

    def generate_economic_indicators(self):
        date_range = pd.date_range(
            start=self.start_date, end=self.end_date, freq='D')
        economic_data = pd.DataFrame(index=date_range)
        economic_data.index.name = "date"

        fred_series = {
            "SP500": "sp500_index",
            "VIXCLS": "vix_index",
            "DGS10": "10yr_treasury_yield",
            "GDP": "gdp_growth_rate",               # rename directly here
            "UNRATE": "unemployment_rate",
            "CPIAUCSL": "inflation_rate",          # rename directly here
            "FEDFUNDS": "federal_funds_rate",
            "MORTGAGE30US": "mortgage_rate_30yr",
            "UMCSENT": "consumer_confidence_index",
            "CSUSHPINSA": "housing_price_index"
        }

        for fred_code, col_name in fred_series.items():
            fred_df = web.DataReader(
                fred_code, "fred", self.start_date, self.end_date)
            fred_df.index = pd.to_datetime(fred_df.index)
            fred_df = fred_df.rename(columns={fred_df.columns[0]: col_name})
            economic_data = economic_data.join(fred_df, how="left")

        economic_data = economic_data.ffill().bfill().reset_index()

        desired_columns = [
            'date',
            'gdp_growth_rate',
            'unemployment_rate',
            'inflation_rate',
            'federal_funds_rate',
            'sp500_index',
            'vix_index',
            'consumer_confidence_index',
            'housing_price_index',
            '10yr_treasury_yield',
            'mortgage_rate_30yr'
        ]

        final_columns = [
            col for col in desired_columns if col in economic_data.columns]
        economic_data = economic_data[final_columns]

        self.economic_indicators = pd.DataFrame(economic_data)
        return self.economic_indicators

    def generate_marketing_campaigns(self, num_campaigns=250):
        schema_marketing_campaigns = [
            {"label": "campaign_id", "key_label": "row_number", "group": 'basic'},
            {"label": "campaign_name", "key_label": "catch_praise", "group": 'personal'},
            {"label": "campaign_type", "key_label": "custom_list", "group": 'basic', "options": {
                "custom_format": "Email,Social Media,Direct Mail,TV,Radio,Online Display"}},
            {
                "label": "start_date",
                "key_label": "datetime",
                "group": "basic",
                "options": {
                    "from_date": self.start_date.strftime('%m/%d/%Y'),
                    "to_date": self.end_date.strftime('%m/%d/%Y'),
                    "date_format": 'yyyy-mm-dd'
                }
            },
            {"label": "end_date", "key_label": "lambda", "group": "advanced", "options": {
                "func": lambda x: pd.to_datetime(x['start_date']) + timedelta(days=random.randint(7, 90))}},
            {"label": "target_segment", "key_label": "custom_list", "group": 'basic',
                "options": {"custom_format": "Mass Market,Affluent,Premium,Business"}},
            {"label": "budget", "key_label": "number", "group": "basic",
                "options": {'min': 10000, 'max': 500000, 'decimals': 2}},
            {"label": "impressions", "key_label": "number",
                "group": "basic", "options": {'min': 10000, 'max': 1000000}},
            {"label": "clicks", "key_label": "number", "group": "basic",
                "options": {'min': 1000, 'max': 50000}},
            {"label": "conversions", "key_label": "number",
                "group": "basic", "options": {'min': 50, 'max': 5000}},
            {"label": "cost_per_acquisition", "key_label": "number",
                "group": "basic", "options": {'min': 50, 'max': 500, 'decimals': 2}},
            {"label": "roi", "key_label": "number", "group": "basic",
                "options": {'min': -0.5, 'max': 3.0, 'decimals': 2}},
            {"label": "product_promoted", "key_label": "number",
                "group": "basic", "options": {'min': 1, 'max': 10}},
        ]

        self.marketing_campaigns = pd.DataFrame(SyntheticDataCrafter(
            schema_marketing_campaigns).many(random.randint(num_campaigns, (num_campaigns * 2))).data)
        self.marketing_campaigns['start_date'] = pd.to_datetime(
            self.marketing_campaigns['start_date'])
        return self.marketing_campaigns

    def generate_loan_payments(self, num_payments=10000):
        loan_accounts = self.accounts[self.accounts['product_id'].isin([
                                                                       4, 5, 8])]
        for _, account in loan_accounts.iterrows():
            payment_date = account['open_date']
            num_payments = random.randint(6, 60)

            for i in range(num_payments):
                if payment_date > self.end_date:
                    break

                scheduled_amount = abs(account['current_balance']) * 0.02
                is_late = random.random() < 0.15
                actual_date = payment_date + \
                    timedelta(days=random.randint(1, 15)
                              ) if is_late else payment_date

                self.loan_payments.append({
                    'payment_id': len(self.loan_payments) + 1,
                    'account_id': account['account_id'],
                    'customer_id': account['customer_id'],
                    'scheduled_date': payment_date,
                    'actual_date': actual_date if random.random() > 0.05 else None,
                    'scheduled_amount': scheduled_amount,
                    'actual_amount': scheduled_amount * random.uniform(0.9, 1.1) if random.random() > 0.05 else 0,
                    'is_late': is_late,
                    'days_late': (actual_date - payment_date).days if is_late else 0,
                    'late_fee': random.uniform(25, 50) if is_late else 0,
                    'payment_method': random.choice(['ACH', 'Check', 'Online', 'Wire Transfer']),
                    'outstanding_balance': abs(account['current_balance']) * random.uniform(0.5, 1.0)
                })

                payment_date += timedelta(days=30)

        self.loan_payments = pd.DataFrame(self.loan_payments)
        return self.loan_payments

    def generate_branch_locations(self, num_branches=500):
        schema_branch_locations = [
            {"label": "_location", "key_label": "lambda", "group": "advanced",
                "options": {"func": lambda: self.us_state.sample(n=1).iloc[0]}},
            {"label": "branch_id", "key_label": "row_number", "group": 'basic'},
            {"label": "branch_name", "key_label": "lambda", "group": "advanced",
                "options": {"func": lambda x: f"{x['_location']['city']} Branch"}},
            {"label": "branch_code", "key_label": "character_sequence",
                "group": "advanced", "options": {'format': "BR#####"}},
            {"label": "branch_type", "key_label": "custom_list", "group": 'basic',
                "options": {"custom_format": "Full Service,Limited Service,Drive-Through Only,Commercial"}},
            {"label": "address", "key_label": "street_address", "group": "location"},
            {"label": "city", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda row: row['_location']['city']}},
            {"label": "state", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda row: row['_location']['state_id']}},
            {"label": "zip_code", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda row: row['_location']['zip']}},
            {"label": "country", "key_label": "lambda",
                "group": "advanced", "options": {'func': lambda: "USA"}},
            {"label": "latitude", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda row: row['_location']['lat']}},
            {"label": "longitude", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda row: row['_location']['lng']}},
            {"label": "phone", "key_label": "phone", "group": "location"},
            {"label": "open_date", "key_label": "datetime", "group": "basic", "options": {
                "from_date": "01/01/1990",
                "to_date": self.end_date.strftime('%m/%d/%Y'),
                "date_format": 'yyyy-mm-dd'
            }},
            {"label": "is_active", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda: random.choice([True, True, True, True, False])}},
            {"label": "square_footage", "key_label": "number",
                "group": "basic", "options": {'min': 1000, 'max': 10000}},
            {"label": "num_employees", "key_label": "number",
                "group": "basic", "options": {'min': 3, 'max': 25}},
            {"label": "avg_daily_customers", "key_label": "number",
                "group": "basic", "options": {'min': 50, 'max': 500}},
            {"label": "has_safe_deposit", "key_label": "boolean", "group": "basic"},
            {"label": "has_notary", "key_label": "boolean", "group": "basic"},
            {"label": "has_coin_counter", "key_label": "boolean", "group": "basic"},
            {"label": "wheelchair_accessible",
                "key_label": "boolean", "group": "basic"},
            {"label": "operating_hours", "key_label": "custom_list", "group": 'basic',
                "options": {"custom_format": "9AM-5PM Mon-Fri,9AM-6PM Mon-Fri,9AM-2PM Sat,24/7"}},
            {"label": "manager_name", "key_label": "full_name", "group": "personal"},
            {"label": "region", "key_label": "custom_list", "group": 'basic',
                "options": {"custom_format": "Northeast,Southeast,Midwest,Southwest,West"}},
        ]

        self.branch_locations = pd.DataFrame(
            SyntheticDataCrafter(schema_branch_locations).many(
                random.randint(num_branches, int(num_branches * 1.5))
            ).data
        )
        self.branch_locations = self.branch_locations.drop(columns=[
                                                           '_location'])
        return self.branch_locations

    def generate_atm_locations(self, num_atms=2000):
        schema_atm_locations = [
            {"label": "_location", "key_label": "lambda", "group": "advanced",
                "options": {"func": lambda: self.us_state.sample(n=1).iloc[0]}},
            {"label": "atm_id", "key_label": "row_number", "group": 'basic'},
            {"label": "atm_code", "key_label": "character_sequence",
                "group": "advanced", "options": {'format': "ATM######"}},
            {"label": "location_name", "key_label": "lambda", "group": "advanced",
                "options": {"func": lambda: random.choice([
                    "Shopping Mall", "Gas Station", "Airport", "Train Station",
                    "Casino", "Hotel", "University", "Hospital", "Grocery Store"
                ])}},
            {"label": "location_type", "key_label": "custom_list", "group": 'basic',
                "options": {"custom_format": "Branch,Off-Site,Third-Party"}},
            {"label": "address", "key_label": "street_address", "group": "location"},
            {"label": "city", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda row: row['_location']['city']}},
            {"label": "state", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda row: row['_location']['state_id']}},
            {"label": "zip_code", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda row: row['_location']['zip']}},
            {"label": "country", "key_label": "lambda",
                "group": "advanced", "options": {'func': lambda: "USA"}},
            {"label": "latitude", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda row: row['_location']['lat']}},
            {"label": "longitude", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda row: row['_location']['lng']}},
            {"label": "install_date", "key_label": "datetime", "group": "basic", "options": {
                "from_date": "01/01/2000",
                "to_date": self.end_date.strftime('%m/%d/%Y'),
                "date_format": 'yyyy-mm-dd'
            }},
            {"label": "is_operational", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda: random.choice([True, True, True, True, False])}},
            {"label": "is_deposit_enabled", "key_label": "boolean", "group": "basic"},
            {"label": "is_cash_only", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda: random.choice([True, False, False, False])}},
            {"label": "max_withdrawal_amount", "key_label": "custom_list", "group": 'basic',
                "options": {"custom_format": "200,300,500,1000"}},
            {"label": "daily_transaction_limit", "key_label": "number",
                "group": "basic", "options": {'min': 5, 'max': 50}},
            {"label": "avg_daily_transactions", "key_label": "number",
                "group": "basic", "options": {'min': 10, 'max': 200}},
            {"label": "cash_capacity", "key_label": "number",
                "group": "basic", "options": {'min': 50000, 'max': 200000}},
            {"label": "last_refill_date", "key_label": "datetime", "group": "basic", "options": {
                "from_date": (self.end_date - pd.DateOffset(days=7)).strftime('%m/%d/%Y'),
                "to_date": self.end_date.strftime('%m/%d/%Y'),
                "date_format": 'yyyy-mm-dd'
            }},
            {"label": "last_maintenance_date", "key_label": "datetime", "group": "basic", "options": {
                "from_date": (self.end_date - pd.DateOffset(days=30)).strftime('%m/%d/%Y'),
                "to_date": self.end_date.strftime('%m/%d/%Y'),
                "date_format": 'yyyy-mm-dd'
            }},
            {"label": "surcharge_fee", "key_label": "custom_list", "group": 'basic',
                "options": {"custom_format": "0.00,1.50,2.00,2.50,3.00"}},
            {"label": "is_24_hour", "key_label": "boolean", "group": "basic"},
            {"label": "has_camera", "key_label": "lambda", "group": "advanced",
                # All ATMs should have cameras
                "options": {'func': lambda: True}},
            {"label": "branch_id", "key_label": "lambda", "group": "advanced",
                "options": {'func': lambda: random.randint(1, 500) if random.random() > 0.6 else None}},
        ]

        self.atm_locations = pd.DataFrame(
            SyntheticDataCrafter(schema_atm_locations).many(
                random.randint(num_atms, int(num_atms * 1.5))
            ).data
        )
        self.atm_locations = self.atm_locations.drop(columns=['_location'])
        return self.atm_locations

    def generate_risk_assessments(self):
        active_customers = self.customers[self.customers['is_active'] == True]
        assessments = []

        for _, customer in active_customers.iterrows():
            num_customer_assessments = random.randint(1, 4)
            assessment_date = customer['signup_date']

            for i in range(num_customer_assessments):
                # Space assessments 3-12 months apart
                if i > 0:
                    assessment_date += timedelta(days=random.randint(90, 365))

                if assessment_date > self.end_date:
                    break

                # Calculate risk factors
                credit_score = customer['credit_score']
                risk_score = round(random.uniform(0, 1), 3)

                # Determine risk rating based on credit score and risk score
                if credit_score >= 750 and risk_score < 0.3:
                    risk_rating = "Low"
                    aml_risk = random.choice(["Low", "Low", "Low", "Medium"])
                elif credit_score >= 650 and risk_score < 0.6:
                    risk_rating = "Medium"
                    aml_risk = random.choice(
                        ["Low", "Medium", "Medium", "High"])
                else:
                    risk_rating = "High"
                    aml_risk = random.choice(
                        ["Medium", "High", "High", "Critical"])

                assessments.append({
                    'assessment_id': len(assessments) + 1,
                    'customer_id': customer['customer_id'],
                    'assessment_date': assessment_date,
                    'assessment_type': random.choice([
                        'Periodic Review', 'Account Opening', 'Transaction Triggered',
                        'Annual Review', 'High Risk Review'
                    ]),
                    'risk_rating': risk_rating,
                    'risk_score': risk_score,
                    'credit_risk': random.choice(["Low", "Medium", "High"]),
                    'fraud_risk': random.choice(["Low", "Low", "Medium", "High"]),
                    'aml_risk': aml_risk,
                    'kyc_status': random.choice([
                        'Verified', 'Verified', 'Verified', 'Pending', 'Expired'
                    ]),
                    'kyc_last_updated': assessment_date - timedelta(days=random.randint(0, 365)),
                    # 5% PEP
                    'pep_flag': random.choice([False] * 95 + [True] * 5),
                    # 2% sanctions
                    'sanctions_flag': random.choice([False] * 98 + [True] * 2),
                    'adverse_media_flag': random.choice([False] * 90 + [True] * 10),
                    'high_value_customer': customer['customer_lifetime_value'] > 50000,
                    'transaction_volume_last_90d': round(random.uniform(1000, 50000), 2),
                    'num_accounts': random.randint(1, 5),
                    'years_as_customer': (assessment_date - customer['signup_date']).days / 365,
                    'employment_verified': random.choice([True, True, True, False]),
                    'income_verified': random.choice([True, True, False]),
                    'address_verified': random.choice([True, True, True, False]),
                    'regulatory_concerns': random.choice([None, None, None, 'OFAC Match', 'Structuring Pattern']),
                    'next_review_date': assessment_date + timedelta(days=random.randint(180, 365)),
                    'assessor_id': f"ASSR{random.randint(1000, 9999)}",
                    'assessment_notes': f"Risk assessment completed for {customer['customer_segment']} customer",
                    'requires_enhanced_due_diligence': risk_rating == "High" or aml_risk in ["High", "Critical"],
                })

        self.risk_assessments = pd.DataFrame(assessments)
        return self.risk_assessments

    def generate_account_events(self):
        events = []
        for _, account in self.accounts.iterrows():
            num_account_events = random.randint(1, 8)
            event_date = account['open_date']
            for i in range(num_account_events):
                if i > 0:
                    event_date += timedelta(days=random.randint(30, 180))

                if event_date > self.end_date:
                    break

                days_since_open = (event_date - account['open_date']).days

                if days_since_open < 30:
                    event_type = random.choice([
                        'Account Opened', 'Initial Deposit', 'Card Activated',
                        'Online Banking Enrolled', 'Mobile App Activated'
                    ])
                elif account['account_status'] == 'Closed' and i == num_account_events - 1:
                    event_type = random.choice([
                        'Account Closed', 'Account Closed - Customer Request',
                        'Account Closed - Inactivity', 'Account Closed - Fraud'
                    ])
                else:
                    event_type = random.choice([
                        'Balance Threshold Crossed', 'Overdraft Occurred',
                        'Credit Limit Increased', 'Credit Limit Decreased',
                        'Interest Rate Changed', 'Fees Waived',
                        'Account Upgraded', 'Account Downgraded',
                        'Autopay Enabled', 'Autopay Disabled',
                        'Statement Delivery Changed', 'Contact Info Updated',
                        'Beneficiary Added', 'Joint Owner Added',
                        'Dormancy Warning', 'Reactivated',
                        'Large Deposit Received', 'Large Withdrawal Made',
                        'Returned Payment', 'NSF Fee Charged',
                        'Maintenance Fee Waived', 'Promotional Rate Applied'
                    ])

                # Get account product info
                product = self.products[self.products['product_id']
                                        == account['product_id']].iloc[0]

                # Generate event-specific details
                if 'Credit Limit' in event_type:
                    old_value = account['credit_limit']
                    change_pct = random.uniform(0.1, 0.5)
                    new_value = old_value * \
                        (1 + change_pct) if 'Increased' in event_type else old_value * \
                        (1 - change_pct)
                elif 'Interest Rate' in event_type:
                    old_value = account['interest_rate']
                    new_value = round(old_value * random.uniform(0.8, 1.2), 4)
                elif 'Balance' in event_type:
                    old_value = None
                    new_value = account['current_balance']
                else:
                    old_value = None
                    new_value = None

                events.append({
                    'event_id': len(events) + 1,
                    'account_id': account['account_id'],
                    'customer_id': account['customer_id'],
                    'product_id': account['product_id'],
                    'event_date': event_date,
                    'event_type': event_type,
                    'event_category': self._categorize_account_event(event_type),
                    'old_value': old_value,
                    'new_value': new_value,
                    'triggered_by': random.choice([
                        'Customer Request', 'System Automated', 'Bank Policy',
                        'Regulatory Requirement', 'Risk Management', 'Promotional Offer'
                    ]),
                    'channel': random.choice([
                        'Online', 'Mobile', 'Branch', 'Phone', 'Mail', 'System'
                    ]),
                    'processed_by': f"EMP{random.randint(1000, 9999)}" if random.random() > 0.5 else None,
                    'notes': f"{event_type} for {product['product_name']} account",
                    'is_reversible': random.choice([True, False]) if 'Closed' not in event_type else False,
                    'requires_approval': event_type in [
                        'Credit Limit Increased', 'Account Upgraded', 'Fees Waived'
                    ],
                    'approval_status': random.choice([
                        'Approved', 'Approved', 'Approved', 'Pending', 'Rejected'
                    ]) if event_type in ['Credit Limit Increased', 'Account Upgraded'] else None,
                })

        self.account_events = pd.DataFrame(events)
        return self.account_events

    def _categorize_account_event(self, event_type):
        if 'Opened' in event_type or 'Activated' in event_type or 'Enrolled' in event_type:
            return 'Account Setup'
        elif 'Closed' in event_type:
            return 'Account Closure'
        elif 'Limit' in event_type or 'Rate' in event_type:
            return 'Terms Change'
        elif 'Upgraded' in event_type or 'Downgraded' in event_type:
            return 'Product Change'
        elif 'Fee' in event_type:
            return 'Fee Related'
        elif 'Overdraft' in event_type or 'NSF' in event_type or 'Returned' in event_type:
            return 'Payment Issue'
        elif 'Dormancy' in event_type or 'Reactivated' in event_type:
            return 'Activity Status'
        else:
            return 'Account Modification'

    def generate_regulatory_reports(self, num_reports=2500):
        report_types = [
            {'code': 'SAR', 'name': 'Suspicious Activity Report',
                'frequency': 'As Needed', 'regulator': 'FinCEN'},
            {'code': 'CTR', 'name': 'Currency Transaction Report',
                'frequency': 'As Needed', 'regulator': 'FinCEN'},
            {'code': 'CIP', 'name': 'Customer Identification Program',
                'frequency': 'As Needed', 'regulator': 'FinCEN'},
            {'code': 'OFAC', 'name': 'OFAC Sanctions Screening',
                'frequency': 'Daily', 'regulator': 'OFAC'},
            {'code': 'BSA', 'name': 'Bank Secrecy Act Report',
                'frequency': 'Quarterly', 'regulator': 'FinCEN'},
            {'code': 'HMDA', 'name': 'Home Mortgage Disclosure Act',
                'frequency': 'Annual', 'regulator': 'CFPB'},
            {'code': 'CRA', 'name': 'Community Reinvestment Act',
                'frequency': 'Annual', 'regulator': 'FDIC'},
            {'code': 'FFIEC', 'name': 'Call Report',
                'frequency': 'Quarterly', 'regulator': 'FFIEC'},
            {'code': 'FDIC', 'name': 'Deposit Insurance Report',
                'frequency': 'Quarterly', 'regulator': 'FDIC'},
            {'code': 'FR-Y9C', 'name': 'Bank Holding Company Report',
                'frequency': 'Quarterly', 'regulator': 'Federal Reserve'},
            {'code': 'AML', 'name': 'Anti-Money Laundering Report',
                'frequency': 'Monthly', 'regulator': 'FinCEN'},
            {'code': 'KYC', 'name': 'Know Your Customer Review',
                'frequency': 'As Needed', 'regulator': 'Internal'},
        ]

        reports = []

        current_date = self.start_date
        while current_date <= self.end_date:
            for _ in range(random.randint(1, 3)):
                report_info = random.choice(report_types)

                customer_id = random.choice(
                    self.customers['customer_id'].tolist()) if random.random() > 0.3 else None
                transaction_id = random.choice(
                    self.transactions['transaction_id'].tolist()) if random.random() > 0.4 else None
                account_id = random.choice(
                    self.accounts['account_id'].tolist()) if random.random() > 0.5 else None

                filing_date = current_date
                due_date = current_date + \
                    timedelta(days=random.randint(15, 90))

                if current_date < self.end_date - timedelta(days=30):
                    filing_status = random.choice([
                        'Filed', 'Filed', 'Filed', 'Filed', 'Filed',
                        'Late Filed', 'Amended', 'Withdrawn'
                    ])
                    actual_filing_date = filing_date + \
                        timedelta(days=random.randint(0, 45))
                else:
                    filing_status = random.choice(
                        ['Filed', 'Pending', 'In Review'])
                    actual_filing_date = filing_date if filing_status == 'Filed' else None

                if report_info['code'] in ['SAR', 'CTR', 'OFAC']:
                    risk_level = random.choice(
                        ['High', 'High', 'Critical', 'Medium'])
                elif report_info['code'] in ['AML', 'BSA']:
                    risk_level = random.choice(
                        ['High', 'Medium', 'Medium', 'Low'])
                else:
                    risk_level = random.choice(['Low', 'Low', 'Low', 'Medium'])

                reports.append({
                    'report_id': len(reports) + 1,
                    'report_type_code': report_info['code'],
                    'report_type_name': report_info['name'],
                    'report_period_start': current_date - timedelta(days=90),
                    'report_period_end': current_date,
                    'filing_date': filing_date,
                    'due_date': due_date,
                    'actual_filing_date': actual_filing_date,
                    'filing_status': filing_status,
                    'report_frequency': report_info['frequency'],
                    'regulator': report_info['regulator'],
                    'customer_id': customer_id,
                    'account_id': account_id,
                    'transaction_id': transaction_id,
                    'amount_reported': round(random.uniform(10000, 5000000), 2) if random.random() > 0.5 else None,
                    'risk_level': risk_level,
                    'requires_follow_up': random.choice([True, False]) if risk_level in ['High', 'Critical'] else False,
                    'follow_up_date': filing_date + timedelta(days=random.randint(30, 90)) if risk_level == 'Critical' else None,
                    'assigned_to': f"COMP{random.randint(100, 999)}",
                    'reviewed_by': f"COMP{random.randint(100, 999)}" if filing_status == 'Filed' else None,
                    'approval_date': actual_filing_date if filing_status == 'Filed' else None,
                    'filing_method': random.choice(['Electronic', 'Electronic', 'Electronic', 'Paper']),
                    'confirmation_number': f"CONF{random.randint(100000, 999999)}" if filing_status == 'Filed' else None,
                    'findings': random.choice([
                        'No Issues Found', 'No Issues Found', 'No Issues Found',
                        'Minor Issues - Corrected', 'Discrepancy Noted',
                        'Requires Additional Review', 'Escalated to Management'
                    ]),
                    'internal_notes': f"{report_info['name']} for period ending {current_date.strftime('%Y-%m-%d')}",
                    'is_amended': filing_status == 'Amended',
                    'original_report_id': random.randint(1, len(reports)) if filing_status == 'Amended' and len(reports) > 0 else None,
                    'penalty_amount': round(random.uniform(1000, 50000), 2) if filing_status == 'Late Filed' and random.random() > 0.7 else None,
                })

            current_date += timedelta(days=random.randint(7, 30))

        self.regulatory_reports = pd.DataFrame(reports[:num_reports])
        return self.regulatory_reports

    def _get_segment_change_reason(self, prev_seg, new_seg, prev_tier, new_tier, prev_risk, new_risk):
        reasons = []

        if prev_seg != new_seg:
            if prev_seg == 'Mass Market' and new_seg == 'Affluent':
                reasons.append('Income Growth')
            elif prev_seg in ['Affluent', 'Premium'] and new_seg == 'Mass Market':
                reasons.append('Balance Decline')
            elif new_seg == 'Premium':
                reasons.append('High Value Customer')
            elif new_seg == 'Business':
                reasons.append('Business Account Conversion')

        if prev_tier != new_tier:
            tier_order = ['Bronze', 'Silver', 'Gold', 'Platinum']
            if tier_order.index(new_tier) > tier_order.index(prev_tier):
                reasons.append('Loyalty Upgrade')
            else:
                reasons.append('Tier Downgrade')

        if prev_risk != new_risk:
            if new_risk == 'High':
                reasons.append('Risk Flag Triggered')
            elif new_risk == 'Low':
                reasons.append('Risk Assessment Improved')

        if not reasons:
            reasons = ['Periodic Review', 'Model Update', 'Policy Change']

        return ', '.join(reasons[:2])

    def generate_customer_segments_history(self, num_changes=25000):
        segment_changes = []

        for _, customer in self.customers.iterrows():
            num_changes_per_customer = random.randint(1, 5)
            change_date = customer['signup_date']

            current_segment = customer['customer_segment']
            current_tier = customer['loyalty_tier']
            current_risk = customer['risk_segment']

            for i in range(num_changes_per_customer):
                if i > 0:
                    change_date += timedelta(days=random.randint(180, 540))

                if change_date > self.end_date:
                    break

                change_type = random.choice([
                    'Segment Change', 'Segment Change', 'Tier Change', 'Risk Change', 'Multiple Changes'
                ])

                previous_segment = current_segment
                previous_tier = current_tier
                previous_risk = current_risk

                if change_type == 'Segment Change' or change_type == 'Multiple Changes':
                    if current_segment == 'Mass Market':
                        current_segment = random.choice(
                            ['Mass Market', 'Affluent', 'Affluent'])
                    elif current_segment == 'Affluent':
                        current_segment = random.choice(
                            ['Mass Market', 'Affluent', 'Premium', 'Premium'])
                    elif current_segment == 'Premium':
                        current_segment = random.choice(
                            ['Affluent', 'Premium', 'Premium', 'Business'])
                    elif current_segment == 'Business':
                        current_segment = random.choice(
                            ['Business', 'Premium'])

                if change_type == 'Tier Change' or change_type == 'Multiple Changes':
                    tier_progression = {
                        'Bronze': 'Silver', 'Silver': 'Gold', 'Gold': 'Platinum', 'Platinum': 'Platinum'}
                    tier_regression = {
                        'Platinum': 'Gold', 'Gold': 'Silver', 'Silver': 'Bronze', 'Bronze': 'Bronze'}

                    if random.random() > 0.3:
                        current_tier = tier_progression.get(
                            current_tier, current_tier)
                    else:
                        current_tier = tier_regression.get(
                            current_tier, current_tier)

                if change_type == 'Risk Change' or change_type == 'Multiple Changes':
                    current_risk = random.choice(['Low', 'Medium', 'High'])

                days_as_customer = (change_date - customer['signup_date']).days
                estimated_ltv = customer['customer_lifetime_value'] * \
                    (days_as_customer / 365 / 10)

                segment_changes.append({
                    'segment_history_id': len(segment_changes) + 1,
                    'customer_id': customer['customer_id'],
                    'effective_date': change_date,
                    'end_date': None,
                    'is_current': i == num_changes_per_customer - 1 and change_date <= self.end_date,
                    'customer_segment': current_segment,
                    'previous_segment': previous_segment if i > 0 else None,
                    'loyalty_tier': current_tier,
                    'previous_tier': previous_tier if i > 0 else None,
                    'risk_segment': current_risk,
                    'previous_risk': previous_risk if i > 0 else None,
                    'change_type': change_type,
                    'change_reason': self._get_segment_change_reason(
                        previous_segment, current_segment, previous_tier, current_tier, previous_risk, current_risk
                    ),
                    'triggered_by': random.choice([
                        'Automated Rule', 'Manual Review', 'Relationship Manager',
                        'Risk Assessment', 'Behavioral Model', 'Campaign Response'
                    ]),
                    'total_accounts': random.randint(1, 8),
                    'total_balance': round(random.uniform(1000, 500000), 2),
                    'avg_monthly_transactions': random.randint(5, 150),
                    'products_held': random.randint(1, 6),
                    'customer_lifetime_value': round(estimated_ltv, 2),
                    'tenure_days': days_as_customer,
                    'credit_score': customer['credit_score'] + random.randint(-50, 50),
                    'annual_income': customer['annual_income'] * random.uniform(0.8, 1.5),
                    'last_interaction_days': random.randint(0, 90),
                    'digital_engagement_score': round(random.uniform(0, 1), 3),
                    'branch_visits_last_90d': random.randint(0, 12),
                    'online_logins_last_90d': random.randint(0, 90),
                    'eligible_for_premium': current_segment in ['Affluent', 'Premium', 'Business'],
                    'churn_risk': random.choice(['Low', 'Medium', 'High']),
                    'cross_sell_opportunity': random.choice([True, False]),
                    'notes': f"{change_type} from {previous_segment} to {current_segment}",
                    'updated_by': f"SYS{random.randint(100, 999)}",
                })

        df = pd.DataFrame(segment_changes)
        df = df.sort_values(['customer_id', 'effective_date'])

        for customer_id in df['customer_id'].unique():
            customer_records = df[df['customer_id']
                                  == customer_id].index.tolist()
            for idx, record_idx in enumerate(customer_records[:-1]):
                next_idx = customer_records[idx + 1]
                df.at[record_idx, 'end_date'] = df.at[next_idx, 'effective_date']
                df.at[record_idx, 'is_current'] = False

        self.customer_segments_history = df
        return self.customer_segments_history

    def generate_all(self, num_transactions):
        """Generate all datasets"""
        print("=" * 60)
        print("ENHANCED FINANCIAL DATA GENERATION")
        print("=" * 60)

        print("\n[1/17] Generating products...")
        self.generate_products()
        print(f"   ✓ Created {len(self.products)} products")

        print("[2/17] Generating merchants...")
        self.generate_merchants()
        print(f"   ✓ Created {len(self.merchants)} merchants")

        print("[3/17] Generating customers...")
        self.generate_customers()
        print(f"   ✓ Created {len(self.customers)} customers")

        print("[4/17] Generating accounts...")
        self.generate_accounts()
        print(f"   ✓ Created {len(self.accounts)} accounts")

        print("[5/17] Generating transactions...")
        self.generate_transactions(num_transactions)
        print(f"   ✓ Created {len(self.transactions)} transactions")

        print("[6/17] Generating credit applications...")
        self.generate_credit_applications()
        print(
            f"   ✓ Created {len(self.credit_applications)} credit applications")

        print("[7/17] Generating fraud alerts...")
        self.generate_fraud_alerts()
        print(f"   ✓ Created {len(self.fraud_alerts)} fraud alerts")

        print("[8/17] Generating customer interactions...")
        self.generate_customer_interactions()
        print(
            f"   ✓ Created {len(self.customer_interactions)} customer interactions")

        print("[9/17] Generating economic indicators...")
        self.generate_economic_indicators()
        print(
            f"   ✓ Created {len(self.economic_indicators)} economic indicator records")

        print("[10/17] Generating marketing campaigns...")
        self.generate_marketing_campaigns()
        print(
            f"   ✓ Created {len(self.marketing_campaigns)} marketing campaigns")

        print("[11/17] Generating loan payments...")
        self.generate_loan_payments()
        print(f"   ✓ Created {len(self.loan_payments)} loan payment records")

        print("[12/17] Generating branch locations...")
        self.generate_branch_locations()
        print(f"   ✓ Created {len(self.branch_locations)} branch locations")

        print("[13/17] Generating ATM locations...")
        self.generate_atm_locations()
        print(f"   ✓ Created {len(self.atm_locations)} ATM locations")

        print("[14//17] Generating risk assessments...")
        self.generate_risk_assessments()
        print(f"   ✓ Created {len(self.risk_assessments)} risk assessments")

        print("[15/17] Generating account events...")
        self.generate_account_events()
        print(f"   ✓ Created {len(self.account_events)} account events")

        print("[16/17] Generating regulatory reports...")
        self.generate_regulatory_reports()
        print(
            f"   ✓ Created {len(self.regulatory_reports)} regulatory reports")

        print("[17/17] Generating customer segments history...")
        self.generate_customer_segments_history()
        print(
            f"   ✓ Created {len(self.customer_segments_history)} segment history records")

        print("\n" + "=" * 60)
        print("DATA GENERATION COMPLETE")
        print("=" * 60)

        return {
            'products': self.products,
            'merchants': self.merchants,
            'customers': self.customers,
            'accounts': self.accounts,
            'transactions': self.transactions,
            'credit_applications': self.credit_applications,
            'fraud_alerts': self.fraud_alerts,
            'customer_interactions': self.customer_interactions,
            'economic_indicators': self.economic_indicators,
            'marketing_campaigns': self.marketing_campaigns,
            'loan_payments': self.loan_payments,
            'branch_locations': self.branch_locations,
            'atm_locations': self.atm_locations,
            'risk_assessments': self.risk_assessments,
            'account_events': self.account_events,
            'regulatory_reports': self.regulatory_reports,
            'customer_segments_history': self.customer_segments_history,
        }

    def save_to_csv(self, datasets, output_dir='data/bronze'):
        import os
        os.makedirs(output_dir, exist_ok=True)

        print(f"\n{'=' * 60}")
        print(f"SAVING DATA TO: {output_dir}")
        print("=" * 60)

        for name, df in datasets.items():
            if len(df) > 0:
                filepath = f'{output_dir}/{name}.csv'
                df.to_csv(filepath, index=False)
                print(f"   ✓ {name}.csv ({len(df):,} rows)")

        print("\n" + "=" * 60)

    def save_to_db(self, datasets):
        engine = create_engine(
            'postgresql://analytics_user:analytics_password@localhost:5432/finance_analytics')

        for name, df in datasets.items():
            df.to_sql(name, engine, schema='bronze',
                      if_exists='replace', index=False)


if __name__ == "__main__":
    generator = FinancialDataGenerator(
        start_date='2010-01-01',
        num_customers=random.randint(10000, 25000)
    )
    datasets = generator.generate_all(num_transactions=100000)
    generator.save_to_csv(datasets, 'data/ingestion')
    generator.save_to_db(datasets)
