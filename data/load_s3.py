import boto3
import pandas as pd
import random
from io import StringIO
from datetime import datetime
from faker import Faker

fake = Faker()
s3 = boto3.client('s3')

def generate_cdc_order_data(num_rows=500):
    data = []
    for _ in range(num_rows):
        order = {
            'order_id': fake.uuid4(),
            'customer_id': fake.uuid4(),
            'order_date': fake.date_this_year(),
            'status': random.choice(['CREATED', 'SHIPPED', 'DELIVERED', 'CANCELLED']),
            'product_id': fake.uuid4(),
            'quantity': random.randint(1, 5),
            'price': round(random.uniform(10.0, 500.0), 2),
            'total_amount': 0.0,
            'cdc_timestamp': datetime.now()
        }
        order['total_amount'] = round(order['quantity'] * order['price'], 2)
        data.append(order)

    df = pd.DataFrame(data)
    return df

def upload_to_s3(bucket_name, file_name, df):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())
    print(f"Data uploaded to s3://{bucket_name}/{file_name}")

df_cdc_order_data = generate_cdc_order_data(num_rows=5000)

bucket_name = ''
file_name = ''

upload_to_s3(bucket_name, file_name, df_cdc_order_data)