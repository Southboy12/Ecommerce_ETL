import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine
import os, dotenv
import numpy as np




category = {'computing': ['computer-accessories', 'computers', 'software'], 
            'electronics': ['accessories-supplies', 'camera-photo', 'cameras', 'ebook-readers-accessories', 'gps-navigation', 'home-audio', 'office-electronics', 'portable-audio-recorders', 'radios-transceivers', 'security-surveillance', 'television-video', 'wearable-technology'],
            'sporting-goods': ['outdoor-adventure', 'outdoor-recreation', 'sports-fitness'], 
            'garden-outdoors': ['farm-ranch', 'gardening-lawn-care', 'generators-portable-power', 'grills-outdoor-cooking', 'outdoor-decor', 'outdoor-heating-cooling', 'outdoor-lighting', 'outdoor-power-tools', 'patio-furniture-accessories', 'pools-hot-tubs-supplies'],
            'musical-instruments': ['amplifiers-effects', 'band-orchestra', 'drums-percussion', 'electronic-music-dj-karaoke', 'guitars', 'instrument-accessories', 'keyboards-midi', 'live-sound-stage', 'microphones-accessories', 'studio-recording-equipment', 'ukuleles, mandolins-banjos', 'wind-woodwind-instruments'],
            'phones-tablets': ['mobile-phone-accessories', 'mobile-phones', 'tablet-accessories', 'tablets', 'telephones-accessories'],
            'toys-games': ['action-figures-statues', 'arts-crafts', 'building-toys', 'dolls-accessories', 'dress-up-pretend-play', 'games', 'hobbies', "kids-electronics", 'learning-education', 'party-supplies', 'puzzles', 'sports-outdoor-play', 'stuffed-animals-plush-toys', 'toy-remote-control-play-vehicles', 'tricycles-scooters-wagons', 'novelty-gag-toys'],
            'fashion': ["kids-fashion", "mens-fashion", 'shoe-jewelry-watch-accessories', 'watches', "womens-fashion"], 
            'pet-supplies': ['birds', 'cats', 'dogs', 'fish-aquatic-pets', 'reptiles-amphibians', 'small-animals'],
            'home-office': ['arts, crafts-sewing', 'home-office-furniture', 'home-kitchen', 'office-products', 'large-appliances-home-improvement'],
            'automobile': ['car-care', 'car-electronics-accessories', 'exterior-accessories', 'interior-accessories', 'lights-lighting-accessories', 'motorcycle-powersports', 'replacement-parts', 'rv-parts-accessories', 'tools-equipment'],
            'health-beauty': ['beauty-personal-care', 'health-care', 'medical-supplies-equipment', 'sexual-wellness', 'vitamins-dietary-supplements', 'wellness-relaxation'],
            'baby-products': ['apparel-accessories', 'baby-toddler-toys', 'bathing-skin-care', 'car-seats-accessories', 'diapering', 'feeding', 'gear', 'gifts', 'health-baby-care', 'nursery', 'potty-training', 'safety', 'strollers-accessories'],
            'industrial-scientific': ['additive-manufacturing-products', 'cutting-tools', 'hydraulics-pneumatics-plumbing', 'industrial-electrical', 'industrial-power-hand-tools', 'janitorial-sanitation-supplies', 'lab-scientific-products', 'packaging-shipping-supplies', 'professional-dental-supplies', 'professional-medical-supplies', 'raw-materials', 'retail-store-fixtures-equipment', 'science-education', 'tapes-adhesives-sealants', 'test-measure-inspect']}




def get_database_conn():
    dotenv.load_dotenv('./.env')
    db_user_name = os.getenv('DB_USER_NAME')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')
    port = os.getenv('DB_PORT')
    host = os.getenv('DB_HOST')
    return create_engine(f'postgresql+psycopg2://{db_user_name}:{db_password}@{host}:{port}/{db_name}')



def get_page():
    product_info = []
    for key, values in category.items():
        for value in values:
            base_url = f'https://www.jumia.com.ng/{value}/all-products/'
            for page_number in range(1, 51):
                url = f'{base_url}?page={page_number}' 
                response = requests.get(url)
                if response.status_code != 200:
                    print(f'{url} returned an error {response.status_code}')
                else:
                    page_content = response.text
                    # print(len(page_content))
                    doc = bs(page_content, 'html.parser')
                    #header_tags = doc.find_all('div', class_='-paxs row _no-g _4cl-3cm-shs')
                    products = doc.find_all('article', class_='prd _fb col c-prd')
                    for product in products:
                        item_name = product.find('h3', class_='name')
                        if item_name is not None:
                            item_name = item_name.text
                            # product_info.append(item_name)
                        else:
                            item_name = ''
                        price = product.find('div', class_='prc')
                        if price is not None:
                            price = price.text.strip().replace('\u20a6', '').replace(',', '').strip()
                            # product_info.append(price)
                        else:
                            price = ''
                        rating = product.find('div', class_='rev')
                        if rating is not None:
                            rating = rating.find('div', class_='stars _s').text.strip().split(' ')[0]
                            # product_info.append(rating)
                        else:
                            rating = ''
                        verified_rating = product.find('div', class_='rev')
                        if verified_rating is not None:
                            verified_rating = verified_rating.text.split('(')[1][:-1]
                            # product_info.append(verified_rating)
                        else:
                            verified_rating = ''
                        old_price = product.find('div', class_='old')
                        if old_price is not None:
                            old_price = old_price.text.strip().replace('\u20a6', '').replace(',', '').strip()
                            # product_info.append(old_price)
                        else:
                            old_price = ''
                        discount = product.find('div', class_='bdg _dsct _sm')
                        if discount is not None:
                            discount = discount.text
                            # product_info.append(discount)
                        else:
                            discount = ''
                        product_info.append([item_name, price, old_price, discount, rating, verified_rating, value, key])
            print(f'Successfully extracted datya for {value}')
    columns = ['item_name', 'price', 'old_price', 'discount', 'rating', 'verified_rating', 'sub-category', 'category']
    df = pd.DataFrame(product_info, columns=columns)
    # print(df.shape)
    return df   



def write_to_local(df):
    file_name = 'jumia_products.csv'
    path = f'raw/{file_name}'
    if os.path.exists('raw') == False:
        os.mkdir('raw')
        df.to_csv(path, index=False)
        # print('data written to file')
    else:
        df.to_csv(path, index=False)
        # print('Data written to file')
    return path


def transform(path):
    df = pd.read_csv('./raw/jumia_products.csv')
    df['price'] = df.price.str.split('-').apply(lambda x: float(x[0]))
    file_name = 'cleaned_jumia_products.csv'
    path = f'raw/{file_name}'
    if os.path.exists('cleaned') == False:
        os.mkdir('cleaned')
        df.to_csv(path, index=False)
        # print('data written to file')
    else:
        df.to_csv(path, index=False)
        print('Data written to file')
    return path

    
def load_to_db(path):
    df = pd.read_csv(path)
    engine = get_database_conn()
    db_table_name = 'jumia_products'
    df.to_sql(db_table_name, con=engine, if_exists='replace', index=False)
    print(f'Successfully written {df.shape[0]} to postgres db')



def parent_function():
    df = get_page()
    path = write_to_local(df)
    clean_path = transform(path)
    load_to_db(clean_path)
    
    


if __name__ == '__main__':
    parent_function()