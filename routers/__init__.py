import logging, time, os
from sqlalchemy import create_engine
from flask import Flask, jsonify, redirect, url_for, request
import datetime as datetime
import pandas as pd

app = Flask(__name__)

def current_milli_time():
    return round(time.time() * 1000)

@app.route("/")
def index():
    return jsonify({ "message": "python-wsgi-function-samples-flask" })

def check_response_field(data):
    for key, value in data.items():
        if value is None:
            data[key] = "null"
        else:
            if type(value) is int:
                data[key] = value
            else:
                data[key] = f"""'{value.replace("'", "''")}'"""
    return data

def convert_date(date):
    logging.info('type ---- : %s', type(date))
    try:
        date = int(float(date))
        return f"""'{datetime.datetime.fromtimestamp(date / 1000.0).date()}'"""
    except:
        return "null"

@app.route("/deals", methods=["GET", "POST"])
def deals():
    server = os.environ["server"]  
    database = os.environ["database"]  
    username = os.environ["un"]  
    password = os.environ["password"]
    schema = os.environ["schema"]

    logging.info("sql os var assigned")

    try:
        SQLALCHEMY_DATABASE_URI = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
        engine = create_engine(SQLALCHEMY_DATABASE_URI, fast_executemany=True)
        logging.info("sql engine created")
    except Exception as e:
        logging.info(f"Failed to create sql engine - {e}")

    if request.method == "POST":
        value = check_response_field(request.get_json())
        try:
            x =  pd.read_sql_query(f"""select * from {schema}.src_hubspot_deals where hs_object_id={value["hs_object_id"]}""",con=engine)
        except Exception as e:
            logging.info(f"Failed to read {e}")
        try:
            dealobject = x["hs_object_id"].iloc[0]
            logging.info(f"---{dealobject}")
            engine.execute(f"""
                update {schema}.src_hubspot_deals 
                set r_code={value["r_code"]} , deal_name={value["deal_name"]}, target_company={value["target_company"]}, fee={value["fee"]}, currency={value["currency"]},
                    engagement_type={value["engagement_type"]},deal_owner_id={value["deal_owner_id"]}, sales_assist_name1={value["sales_assist"]}, project_leader={value["project_leader"]}, 
                    deal_lost_reason_type={value["deal_lost_reason_type"]}, 
                    proposal_date={convert_date(value["proposal_date"])}, 
                    create_date={convert_date(value["create_date"])},
                    last_modified_date={convert_date(value["last_modified_date"])},
                    deal_type={value["deal_type"]}, deal_client_name={value["deal_client_name"]}, deal_industry={value["deal_industry"]}, deal_sub_sector1={value["deal_sub_sector1"]}, 
                    deal_sub_sector2={value["deal_sub_sector2"]}, office={value["office"]}, sharepoint_project_site_url={value["sharepoint_project_site_url"]}, amount={value["amount"]}
                where hs_object_id={value["hs_object_id"]}
                """)
           
            if value["industry"] is not None:
                l = value["industry"].split(';')
                engine.execute(f"""
                    DELETE FROM {schema}.src_hubspot_deal_industries
                    WHERE deal_id={value["hs_object_id"]};
                """)
                for i in (l):
                    time.sleep(0.5)
                    engine.execute(f"""
                        INSERT INTO {schema}.src_hubspot_deal_industries (id,industry,deal_id)
                        VALUES({current_milli_time()},'{i.replace("'", "")}',{value["hs_object_id"]});
                    """)
            return jsonify({ "message":"Successfully Updated"})
        except Exception as e:
            logging.info(f"Exception ----- {e}") 
            engine.execute(f"""
                INSERT INTO {schema}.src_hubspot_deals
                (hs_object_id, r_code, deal_name, amount, fee, currency, deal_client_name, create_date, proposal_date, last_modified_date, office, deal_owner_id, project_leader, deal_type, deal_industry, 
                deal_sub_sector1, deal_sub_sector2, target_company, engagement_type, sales_assist_name1, deal_lost_reason_type, sharepoint_project_site_url)
                VALUES({value["hs_object_id"]}, {value["r_code"]}, {value["deal_name"]}, {value["amount"]}, {value["fee"]}, {value["currency"]}, {value["deal_client_name"]}, 
                {convert_date(value["create_date"])}, {convert_date(value["proposal_date"])}, {convert_date(value["last_modified_date"])}, {value["office"]}, {value["deal_owner_id"]},
                {value["project_leader"]}, {value["deal_type"]}, {value["deal_industry"]}, {value["deal_sub_sector1"]}, {value["deal_sub_sector2"]}, {value["target_company"]}, {value["engagement_type"]}, '',{value["deal_lost_reason_type"]}, {value["sharepoint_project_site_url"]});
                """)
            if value["industry"] is not None:
                l = value["industry"].split(';')
                for i in (l):
                    time.sleep(0.5)
                    engine.execute(f"""
                        INSERT INTO {schema}.src_hubspot_deal_industries (id,industry,deal_id)
                        VALUES({current_milli_time()},'{i.replace("'", "")}',{value["hs_object_id"]});
                    """)
            return jsonify({ "message":"Successfully created"})
    else:
        logging.info('Called GET method')
        return jsonify({ "message": "python-wsgi-function-samples-flask" })
