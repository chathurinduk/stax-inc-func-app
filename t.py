import logging

import azure.functions as func

from sqlalchemy import create_engine
import pandas as pd
import datetime as datetime
import os
import pandas as pd
from fastapi import FastAPI

app = FastAPI()

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    if req.method == 'POST':
        req_body = req.get_json()
        load_json(req_body)
        return func.HttpResponse(
             "called funcionary"
        )
    else:
        return func.HttpResponse(
             "Method Not Allowed",
             status_code=405
        )

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

def load_json(value):
    check_response_field(value)

    server = os.environ["server"]  
    database = os.environ["database"]  
    username = os.environ["un"]  
    password = os.environ["password"]

    logging.info("sql os var assigned")

    try:
        SQLALCHEMY_DATABASE_URI = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
        engine = create_engine(SQLALCHEMY_DATABASE_URI, fast_executemany=True)
        logging.info("sql engine created")
    except Exception as e:
        logging.info(f"Failed to create sql engine - {e}")

    # try:
    #     engine.execute(f"""
    #         update mdm_web.man_projects 
    #         set budgeted_amount={value["amount"]}
    #         where project_code='{value["r_code"]}'
    #         """)
    # except Exception as e:
    #     logging.info(f"Failed to Update budgeted amount {e}")
    
    try:
        x =  pd.read_sql_query(f"""select * from mdm_web.src_hubspot_deals where hs_object_id='{value["hs_object_id"]}'""",con=engine)
    except Exception as e:
        logging.info(f"Failed to read {e}")

    # if value["r_code"] == None:
    #     r_code = """r_code=null"""
    #     r_code_i = """null"""
    # else:
    #     r_code = f"""r_code='{value["r_code"]}'"""
    #     r_code_i = f"""'{value["r_code"]}'"""

    # if value["deal_name"] == None:
    #     deal_name = """deal_name=null"""
    #     deal_name_i = """null"""
    # else:
    #     deal_name = f"""deal_name='{(value["deal_name"]).replace("'", "''")}'"""
    #     deal_name_i = f"""'{(value["deal_name"]).replace("'", "''")}'"""

    # if value["fee"] is None or value["fee"] == '':
    #     fee = """fee=null"""
    #     fee_i = """null"""
    # else:
    #     fee = f"""fee={value["fee"]}"""
    #     fee_i = f"""{value["fee"]}"""

    # if value["deal_client_name"] == None:
    #     deal_client_name = """deal_client_name=null"""
    #     deal_client_name_i = """null"""
    # else:
    #     deal_client_name = f"""deal_client_name='{(value["deal_client_name"]).replace("'", "''")}'"""
    #     deal_client_name_i = f"""'{(value["deal_client_name"]).replace("'", "''")}'"""
    
    # try:
    #     dealobject = x["hs_object_id"].iloc[0]
    #     print(dealobject)
    #     try:
    #         engine.execute(f"""
    #         update mdm_web.src_hubspot_deals 
    #         set hs_object_id={value["hs_object_id"]} , {r_code} , {deal_name} , {fee} ,{deal_client_name} ,createdate='{datetime.datetime.fromtimestamp(value["createdate"] / 1000.0).date()}' ,last_modified_date='{datetime.datetime.fromtimestamp(value["last_modified_date"] / 1000.0).date()}'
    #         where hs_object_id={value["hs_object_id"]}
    #         """)
    #         logging.info("Successfully updated")
    #         # return func.HttpResponse(f"Successfully updated {str(value)}",status_code=201)
    #     except Exception as e:
    #         logging.info(f"Failed to update {e}")
    #         # return func.HttpResponse("Failed to update",status_code=500)
    # except:
    #     try:
    #         engine.execute(f"""
    #         INSERT INTO mdm_web.src_hubspot_deals (hs_object_id,r_code,deal_name,fee,deal_client_name,createdate,last_modified_date) 
    #         VALUES({value["hs_object_id"]},{r_code_i},{deal_name_i},{fee_i},{deal_client_name_i},'{datetime.datetime.fromtimestamp(value["createdate"] / 1000.0).date()}','{datetime.datetime.fromtimestamp(value["last_modified_date"] / 1000.0).date()}')
    #         """)
    #         logging.info("Successfully added")
    #         # return func.HttpResponse(f"Successfully inserted {str(value)}",status_code=201)
    #     except Exception as e:
    #         logging.info(f"Failed to insert {e}")
    #         # return func.HttpResponse("Failed to Insert",status_code=500)












    import logging

import azure.functions as func
from . import routers

from sqlalchemy import create_engine
import pandas as pd
import datetime as datetime
import os
from fastapi import FastAPI, Request

app = FastAPI()
# app.include_router(deals.router, prefix="/deals")

@app.get("/sample")
async def index():
    return {
        "info": "Try /hello/Shivani for parameterized route.",
    }


@app.get("/hello/{name}")
async def get_name(name: str):
    return {
        "name": name,
    }

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

@app.post("/deals")
async def getInformation(req: Request):
    try:
        logging.info('type is %s', type(req))
    except Exception as e:
        logging.error('error got - %s',e)
    return {
        "status" : "SUCCESS",
        "data" : req
    }


async def main(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    """Each request is redirected to the ASGI handler."""
    return await func.AsgiMiddleware(app).handle_async(req, context)

# def main(req: func.HttpRequest) -> func.HttpResponse:
#     logging.info('Python HTTP trigger function processed a request.')


#     if req.method == 'POST':
#         req_body = req.get_json()
#         load_json(req_body)
#         return func.HttpResponse(
#              "called funcionary"
#         )
#     else:
#         return func.HttpResponse(
#              "Method Not Allowed",
#              status_code=405
#         )

# def check_response_field(data):
#     for key, value in data.items():
#         if value is None:
#             data[key] = "null"
#         else:
#             if type(value) is int:
#                 data[key] = value
#             else:
#                 data[key] = f"""'{value.replace("'", "''")}'"""
#     return data


# def load_json(value):
#     logging.info('value: %s', value)
#     cleaned_value = check_response_field(value)
#     logging.info('type of value: %s', type(value))
#     logging.info('cleaned value: %s', cleaned_value)
    
#     # async def index():
#     return {
#         "info": "Try /hello/Shivani for parameterized route.",
#     }
