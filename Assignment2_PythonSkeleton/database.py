#!/usr/bin/env python3
import psycopg2
from datetime import date,datetime

#####################################################
##  Database Connection
#####################################################

'''
Connect to the database using the connection string
'''
def openConnection():
    # connection parameters - ENTER YOUR LOGIN AND PASSWORD HERE

    myHost = "localhost"
    userid = "postgres"
    passwd = "Ab488700"
    dbname = "COMP9120 Assignment2"
    
    # Create a connection to the database
    conn = None
    try:
        # Parses the config file and connects using the connect string
        conn = psycopg2.connect(database=dbname,
                                    user=userid,
                                    password=passwd,
                                    host=myHost)

    except psycopg2.Error as sqle:
        print("psycopg2.Error : " + sqle.pgerror)
    
    # return the connection to use
    return conn

'''
Validate salesperson based on username and password
'''
def checkLogin(login, password):
    conn = openConnection()
    cur = conn.cursor()

    try:
        # query = f"""
        #         SELECT username, firstname, lastname
        #         FROM Salesperson
        #         WHERE LOWER(username) = LOWER(%s)
        #         AND password = %s
        #         """
        query = "SELECT * FROM check_login(%s, %s);"
        cur.execute(query, (login, password))
        result = cur.fetchone()

        if result:
            return list(result)
        else:
            return None
    except Exception as e:
        print("Login error:", e)
        return None
    finally:
        cur.close()
        conn.close()


"""
    Retrieves the summary of car sales.

    This method fetches the summary of car sales from the database and returns it 
    as a collection of summary objects. Each summary contains key information 
    about a particular car sale.

    :return: A list of car sale summaries.
"""
def getCarSalesSummary():
    conn = openConnection()
    cur = conn.cursor()

    try:
        # query = """
        #         SELECT
        #             mk.MakeName,
        #             mo.ModelName,
        #             SUM(CASE WHEN cs.IsSold = FALSE THEN 1 ELSE 0 END) AS AvailableUnits,
        #             SUM(CASE WHEN cs.IsSold = TRUE THEN 1 ELSE 0 END) AS SoldUnits,
        #             SUM(CASE WHEN cs.IsSold = TRUE THEN cs.Price ELSE 0 END) AS TotalSales,
        #             MAX(CASE WHEN cs.IsSold = TRUE THEN cs.SaleDate ELSE NULL END) AS LastPurchasedAt
        #         FROM CarSales cs
        #         JOIN Make mk ON cs.MakeCode = mk.MakeCode
        #         JOIN Model mo ON cs.ModelCode = mo.ModelCode
        #         GROUP BY mk.MakeName, mo.ModelName
        #         ORDER BY mk.MakeName ASC, mo.ModelName ASC;
        #         """

        query="SELECT * FROM get_car_sales_summary();"
        cur.execute(query)
        result = cur.fetchall()

        summary_list = []
        for row in result:
            summary_list.append({
                'make': row[0],
                'model': row[1],
                'availableUnits': row[2],
                'soldUnits': row[3],
                'soldTotalPrices': float(row[4]),
                'lastPurchaseAt': row[5].strftime('%d-%m-%Y') if isinstance(row[5], date) else ''
            })

        return summary_list

    except Exception as e:
        print("Summary error:", e)
        return None

    finally:
        cur.close()
        conn.close()

"""
    Finds car sales based on the provided search string.

    This method searches the database for car sales that match the provided search 
    string. See assignment description for search specification

    :param search_string: The search string to use for finding car sales in the database.
    :return: A list of car sales matching the search string.
"""
def findCarSales(searchString):
    conn = openConnection()
    cur = conn.cursor()

    try:
        like_pattern=f"%{searchString.lower()}%"
        query = """
                SELECT
                    cs.CarSaleID,
                    mk.MakeName,
                    mo.ModelName,
                    cs.BuiltYear,
                    cs.Odometer,
                    cs.Price,
                    cs.IsSold,
                    CONCAT(cu.FirstName, ' ', cu.LastName) AS CustomerName,
                    CONCAT(sp.FirstName, ' ', sp.LastName) AS SalespersonName,
                    cs.SaleDate
                FROM CarSales cs
                JOIN Make mk on cs.MakeCode = mk.MakeCode
                JOIN Model mo ON cs.ModelCode = mo.ModelCode
                LEFT JOIN Customer cu ON cs.BuyerID = cu.CustomerID
                LEFT JOIN Salesperson sp ON cs.SalespersonID = sp.UserName
                WHERE (LOWER(mk.MakeName) LIKE %s OR
                    LOWER(mo.ModelName) LIKE %s OR
                    LOWER(CONCAT(cu.FirstName, ' ', cu.LastName)) LIKE %s OR
                    LOWER(CONCAT(sp.FirstName, ' ', sp.LastName)) LIKE %s 
                         ) 
                AND (cs.IsSold = FALSE OR
                    cs.SaleDate >= CURRENT_DATE - INTERVAL '3 years')
                ORDER BY cs.IsSold ASC,
                    cs.SaleDate DESC NULLS LAST
                    
                """

        cur.execute(query, (like_pattern, like_pattern, like_pattern, like_pattern))
        result = cur.fetchall()

        sales_list = []
        for row in result:
            sales_list.append({
                'carsale_id': row[0],
                'make': row[1],
                'model': row[2],
                'builtYear': row[3],
                'odometer': row[4],
                'price': float(row[5]),
                'isSold': row[6],
                'buyer': row[7] if row[7] else '',
                'salesperson': row[8] if row[8] else '',
                'sale_date':  row[9].strftime('%d-%m-%Y') if isinstance(row[9], date) else ''
            })

        return sales_list

    except Exception as e:
        print("findCarSales error:", e)
        return None

    finally:
        cur.close()
        conn.close()

"""
    Adds a new car sale to the database.

    This method accepts a CarSale object, which contains all the necessary details 
    for a new car sale. It inserts the data into the database and returns a confirmation 
    of the operation.

    :param car_sale: The CarSale object to be added to the database.
    :return: A boolean indicating if the operation was successful or not.
"""
def addCarSale(make, model, builtYear, odometer, price):
    conn = openConnection()
    cur = conn.cursor()

    try:
        #find Makecode and Modelcode
        query1= '''
                SELECT mo.ModelCode, mk.MakeCode
                FROM Model mo
                JOIN Make mk ON mo.MakeCode = mk.MakeCode
                WHERE LOWER(mk.MakeName) = LOWER(%s) AND LOWER(mo.ModelName) = LOWER(%s)
        '''
        cur.execute(query1,(make,model))
        result = cur.fetchone()

        if not result:
            print('Cannot find suitable Make or Model')
            return False

        modelCode,makeCode = result

        #insert new carsale record
        query2 = '''
                INSERT INTO CarSales(MakeCode, ModelCode, BuiltYear, Odometer, Price,IsSold)
                VALUES(%s, %s, %s, %s, %s, FALSE)
        '''

        cur.execute(query2,(makeCode,modelCode,builtYear,odometer,price))

        conn.commit()
        return True


    except Exception as e:
        print("addCarSale error:", e)
        conn.rollback()
        return False

    finally:
        cur.close()
        conn.close()
"""
    Updates an existing car sale in the database.

    This method updates the details of a specific car sale in the database, ensuring
    that all fields of the CarSale object are modified correctly. It assumes that 
    the car sale to be updated already exists.

    :param car_sale: The CarSale object containing updated details for the car sale.
    :return: A boolean indicating whether the update was successful or not.
"""
def updateCarSale(carsaleid, customer, salesperosn, saledate):
    conn = openConnection()
    cur = conn.cursor()

    try:
        # trans date format
        sale_date_obj = datetime.strptime(saledate, "%Y-%m-%d").date()

        salesperosn=salesperosn.lower().strip()
        customer=customer.lower().strip()
        # Check if cusotmor valid
        cur.execute("SELECT 1 FROM Customer WHERE LOWER(CustomerID) = %s", (customer,))
        if not cur.fetchone():
            print(f"Customer ID '{customer}' not found.")
            return False

        # Check if salesperson valid
        cur.execute("SELECT 1 FROM Salesperson WHERE LOWER(UserName) = %s", (salesperosn,))
        if not cur.fetchone():
            print(f"Salesperson username '{salesperosn}' not found.")
            return False

        # renew carsales table
        query='''
                UPDATE CarSales 
                SET BuyerID = %s,
                    SalespersonID = %s,
                    SaleDate = %s,
                    IsSold = %s
                WHERE CarSaleID = %s
                AND %s <= CURRENT_DATE
        '''
        cur.execute(query, (customer, salesperosn, sale_date_obj,True, carsaleid, sale_date_obj))

        #check if update sucessful
        if cur.rowcount == 0:
            print(f"Update failed: Sale date {sale_date_obj} is invalid (future) or CarSaleID not found.")
            return False

        conn.commit()
        return True

    except Exception as e:
        print("updateCarSale error:", e)
        conn.rollback()
        return False

    finally:
        cur.close()
        conn.close()
