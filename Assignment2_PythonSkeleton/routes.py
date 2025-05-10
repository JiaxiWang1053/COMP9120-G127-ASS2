# Importing the frameworks
from flask import *
from datetime import datetime
import database

user_details = {}
session = {}
page = {}

# Initialise the application
app = Flask(__name__)
app.secret_key = 'aab12124d346928d14710610f'


#####################################################
##  INDEX
#####################################################

@app.route('/')
def index():
    # Check if the user is logged in
    if('logged_in' not in session or not session['logged_in']):
        return redirect(url_for('login'))
    page['title'] = 'The Sydney Automotive Group'
    
    return redirect(url_for('summary'))

    #return render_template('index.html', session=session, page=page, user=user_details)

#####################################################
##  LOGIN
#####################################################

@app.route('/login', methods=['POST', 'GET'])
def login():
    # Check if they are submitting details, or they are just logging in
    if (request.method == 'POST'):
        # submitting details
        login_return_data = check_login(request.form['id'], request.form['password'])

        # If they have incorrect details
        if login_return_data is None:
            page['bar'] = False
            flash("Incorrect login info, please try again.")
            return redirect(url_for('login'))

        # Log them in
        page['bar'] = True
        welcomestr = 'Welcome back, ' + login_return_data['firstName'] + ' ' + login_return_data['lastName']
        flash(welcomestr)
        session['logged_in'] = True

        # Store the user details
        global user_details
        user_details = login_return_data
        return redirect(url_for('index'))

    elif (request.method == 'GET'):
        return(render_template('login.html', page=page))

#####################################################
##  LOGOUT
#####################################################

@app.route('/logout')
def logout():
    session['logged_in'] = False
    page['bar'] = True
    flash('You have been logged out. See you soon!')
    return redirect(url_for('index'))

#####################################################
##  Summary
#####################################################
@app.route('/summary', methods=['POST', 'GET'])
def summary():
    # Check if user is logged in
    if ('logged_in' not in session or not session['logged_in']):
        return redirect(url_for('login'))
    
    summary = database.getCarSalesSummary()
    if (summary is None):
        summary = []
        flash("There are no summary in the system for " + user_details['firstName'] + " " + user_details['lastName'])
        page['bar'] = False
    return render_template('summary.html', summary=summary, session=session, page=page)

#####################################################
##  List Car Sales
#####################################################
@app.route('/list_carsales', methods=['POST', 'GET'])
def list_carsales():
    # Check if user is logged in
    if ('logged_in' not in session or not session['logged_in']):
        return redirect(url_for('login'))

    # User selects a row in the Car Sales Summary
    if (request.method == 'GET'):
        search = request.args.get('search')
        carsale_list = database.findCarSales(search)
        if (carsale_list is None):
            carsale_list = []
            flash('There are no records in the system for search key word "' + user_details['firstName'] + " " + user_details['lastName'])
            page['bar'] = False
        return render_template('list_carsales.html', carsale_list=carsale_list, session=session, page=page)
    elif (request.method == 'POST'): # Users is searching
        search_term = request.form['search']
        if (search_term == ''): # Searching with a blank or empty keyword field
            carsale_list_find = database.findCarSales(f"{user_details['firstName']} {user_details['lastName']}")
        else:    
            carsale_list_find = database.findCarSales(search_term)
        if (carsale_list_find is None):
            carsale_list_find = []
            flash("Searching \'{}\' does not return any result".format(request.form['search']))
            page['bar'] = False
    return render_template('list_carsales.html', carsale_list=carsale_list_find, session=session, page=page)


#####################################################
##  Add carsale
#####################################################

@app.route('/new_carsale' , methods=['GET', 'POST'])
def new_carsale():
    # Check if the user is logged in
    if ('logged_in' not in session or not session['logged_in']):
        return redirect(url_for('login'))

    # If we're just looking at the 'new carsale' page
    if(request.method == 'GET'):
        times = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]
        return render_template('new_carsale.html', user=user_details, times=times, session=session, page=page)

	# If we're adding a new car sale
    success = database.addCarSale(request.form['make'],
                                request.form['model'],
                                request.form['builtyear'],
                                request.form['odometer'],
                                request.form['price'])
    if(success == True):
        page['bar'] = True
        flash("New car added for sale!")
        return(redirect(url_for('index')))
    else:
        page['bar'] = False
        flash("There was an error adding a new car for sale.")
        return(redirect(url_for('new_carsale')))

#####################################################
## Update Sale
#####################################################
@app.route('/update_carsale', methods=['GET', 'POST'])
def update_carsale():
    # Check if the user is logged in
    if ('logged_in' not in session or not session['logged_in']):
        return redirect(url_for('login'))

    # If we're just looking at the 'update carsale' page
    if (request.method == 'GET'):

        datelen = len(request.args.get('sale_date'))
        if (datelen > 0):
            sale_date = datetime.strptime(request.args.get('sale_date'), '%d-%m-%Y').date()
        else:
            sale_date = ''

        # Get the carsale
        carsale = {
            'carsale_id': request.args.get('carsale_id'),
            'make': request.args.get('make'),
            'model': request.args.get('model'),
            'customer_name': request.args.get('customer'),
			'salesperson_name': request.args.get('salesperson'),
            'sale_date': sale_date
        }

        # If there is no carsale
        if carsale['carsale_id'] is None:
            carsale = []
		    # Do not allow viewing if there is no admission to update
            page['bar'] = False
            flash("You do not have access to update that record!")
            return(redirect(url_for('index')))

	    # Otherwise, if admission details can be retrieved
        times = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]
        return render_template('update_carsale.html', carsaleInfo=carsale, user=user_details, times=times, session=session, page=page)

    # If we're updating sale
    sale_date = request.form['sale_date']
    if (sale_date == ''):
        sale_date = None

    success = database.updateCarSale(request.form['carsale_id'],
                                        request.form['customer'],
                                        request.form['salesperson'],
                                        sale_date)
    if (success == True):
        page['bar'] = True
        flash("Sale record updated!")
        return(redirect(url_for('index')))
    else:
        page['bar'] = False
        flash("There was an error updating the carsale.")
        return(redirect(url_for('index')))


def check_login(login, password):
    userInfo = database.checkLogin(login, password)

    if userInfo is None:
        return None
    else:
        tuples = {
            'login': userInfo[0],
            'firstName': userInfo[1],
            'lastName': userInfo[2]
        }
        return tuples
