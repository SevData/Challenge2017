
'''
FILE : flag_anomalies.py
AUTHOR : S BOUSSIE
DESCRIPTION : python3 script for insight data engineering challenge
ARGUMENTS : batch_log_file, stream_log_file, output_file
LINUX command line example: python3 ./src/flag_anomalies.py ./log_input/batch_log.json ./log_input/stream_log.json ./log_output/flagged_purchases.json
'''


#######################################################################
### A. PYTHON STANDARD LIBRARIES IMPORT
#######################################################################

import json
from math import sqrt
import sys
from collections import OrderedDict, deque
import time
from bisect import bisect_left

#######################################################################
### B. INITIALIZE GLOBAL DATA STRUCTURES
#######################################################################
# D & T parameters
D = 0
T = 0

# Time probes
time_social = 0
time_transaction = 0
time_compute = 0

### social_data will contain the list of all friends for each user ID
social_data = {}

### purchase_data will contain the list of all friends for each user ID
purchase_data = {}
purchase_counter = 0



#######################################################################
### C. FUNCTIONS DECLARATION
#######################################################################


#######################################
# C.1  check_add_user 
# DESCRIPTION : adds a uid to the data structures if not present
# INPUT : user id
# OUTPUT : na

def check_add_user(uid) :
	global social_data
	global purchase_data
	if uid not in social_data  :
		social_data[uid] = []
		purchase_data[uid] = []
	return



#######################################
# C.2  update_data 
# DESCRIPTION :  updates the social and purchase data based on the event. Shared by both batch and stream log process
# INPUT : event from both batch and stream logs
# OUTPUT : 'P' if a purchase is successfully logged, True if befriend or unfriend successfully logged , False if error


def update_data(event) :
	global purchase_counter
	global social_data
	global purchase_data
	
	### Check if event_type is present, this will be the main driver to take the appropriate action
	if 'event_type' not in event :
		print('LINE ERROR - log event not processed', event)
		return False

	### process Purchase event
	if event['event_type'] == 'purchase' :
		### Check if all the necessary fields are present in the json 
		for field in ['amount', 'id', 'timestamp']:
			if field not in event:
				print('LINE ERROR - missing field in purchase:', field,event)
				return False

		### Initialize data structure for new user
		uid = int(event['id'])
		check_add_user(uid)

		### Fill purchase_data with purchase information , we change the timestamp to a unique counter
		try : 
			amount = float(event['amount'])
		except :
			print ('LINE ERROR Invalid purchase amount',event)
			return False

		if amount < 0 :
			print ('LINE ERROR, Negative purchase amount',event)
			return False			

		purchase_data[uid].append((purchase_counter,float(event['amount'])))
		if len(purchase_data[uid]) > T : 
			purchase_data[uid].pop(0)
		purchase_counter += 1
		return 'P'

	if event['event_type'] == 'befriend' :
		### Check if all the necessary fields are present in the json 
		for field in ['id1', 'id2', 'timestamp']:
			if field not in event:
				print('LINE ERROR - missing field in befriend:', field,event)
				return False

		### Initialize data structure for new users
		uid1 = int(event['id1'])
		uid2 = int(event['id2'])
		check_add_user(uid1)
		check_add_user(uid2)

		### add each user to the other one's friends list if they are not yet friends
		if uid1 not in social_data[uid2] : social_data[uid2].append(uid1)
		if uid2 not in social_data[uid1] : social_data[uid1].append(uid2)		

		return True

	if event['event_type'] == 'unfriend' :
		### Check if all the necessary fields are present in the json 
		for field in ['id1', 'id2', 'timestamp']:
			if field not in event:
				print('LINE ERROR - missing field in unfriend:', field,event)
				return False

		### Initialize data structure for new users
		uid1 = int(event['id1'])
		uid2 = int(event['id2'])
		check_add_user(uid1)
		check_add_user(uid2)

		### remove each user to the other one's friends list
		if uid1 in social_data[uid2] : social_data[uid2].remove(uid1)
		if uid2 in social_data[uid1] : social_data[uid1].remove(uid2)		

		return True

	print('LINE ERROR, Event not recognized',event)


#######################################
# C.3  flag purchase
# DESCRIPTION :  find anomaly in a purchase 
# INPUT : event from the stream logs
# OUTPUT : False if the purchase is not an anomaly. If it is an anomaly, the function returns the event enriched with mean and std


def flag_purchase(event) :
	global social_data
	global purchase_data
	global time_social 
	global time_transaction
	global time_compute


	#### First step is to build the Dth-degree netowrk
	s = time.time()

	uid = int(event['id'])

	# queue initialization
	d = deque()
	d.append(uid)
	user_network = {}
	user_network[uid] = 0 # reflect the degree 

	# Breadth-first search algo
	while len(d) > 0  :

		current_node = d[0]
		next_degree = user_network[current_node] +1 
		for friend in social_data[current_node] :
			if friend not in user_network :
				user_network[friend] = next_degree
				if next_degree < D :
					d.append(friend)

		d.popleft()

	user_network = list(user_network.keys())
	user_network.remove(uid)

	# incrment social network performance indicator
	time_social += time.time() -s  

	s = time.time()


	### 2nd step is to Collect T last transaction in the network 

	# List initialization
	transactions_amount = []
	transactions_counter = []

	for friend in user_network :
		# we load the last T transaction of each friend

		friend_transactions = purchase_data[friend]
		nb_transaction = len(friend_transactions)
		for i in range(min(nb_transaction,T)) :
			# begin by most recent transactions 
			new_transaction = friend_transactions[nb_transaction - i -1]
			new_transaction_counter = new_transaction[0]
			new_transaction_amount = new_transaction[1]

			if len(transactions_counter) < T :
				index = bisect_left(transactions_counter,new_transaction_counter)
				transactions_counter.insert(index,new_transaction_counter)
				transactions_amount.insert(index,new_transaction_amount)	

			else:
				index = bisect_left(transactions_counter,new_transaction_counter)
				if index == 0 : # we break as soon as the transactions are older than the oldest transaction
					break
				else:
					transactions_counter.insert(index,new_transaction_counter)
					transactions_amount.insert(index,new_transaction_amount)									
					transactions_counter.pop(0)
					transactions_amount.pop(0)


	# increment Transaction collection performance indicator
	time_transaction += time.time() -s  


	# check if there is enough transactions to trigger an anomaly
	len_transactions = len(transactions_counter)
	if len_transactions <2 : 
		print('Not enough transactions')
		return False

	## Compute std and mean and anomaly threshold
	s = time.time()

	mean = sum(transactions_amount)/len_transactions

	std = 0
	for i in range(len_transactions):
		std += (transactions_amount[i] - mean)**2

	std = sqrt(std/len_transactions)

	threshold = mean + 3 * std

	# increment std/mean performance indicator
	time_compute += time.time() -s  


	# Return Anomaly if amount over threshold
	if float(event['amount']) >= threshold :

		event['mean'] = '%.2f'  % mean
		event['sd'] = '%.2f' % std

		return event


	return False


#######################################################################
#######################################################################
### D. MAIN
#######################################################################
#######################################################################


#######################################
# D.1  Collect Process Arguments passed by the unix shell

args = eval(str(sys.argv))
if len(args)!=4:
	print('ERROR problem, not enough arguments')
else :
	batch_log_file = args[1]
	stream_log_file = args[2]
	output_file = args[3]


#######################################
# D.2  Load BATCH LOGS json and Build initial data structure

start = time.time()

with open(batch_log_file,'r') as f :
	
	### READ THE FIRST LINE AND SET THE PARAMETERS D and T
	try:
		first_line = json.loads(f.readline())
	except :
		print('ERROR, 1st line not in recognized JSON format')
		exit()
	
	try :
		D = int(first_line['D'])
	except:
		print ('ERROR : Parameter D error in Batch log')
		exit()
	print('Parameter D found :',D)

	if D == 0 :
		print('ERROR : Parameter D at zero')
		exit()

	try:
		T = int(first_line['T'])
	except:
		print ('ERROR : Parameter T error in Batch log')
		exit()

	print('Parameter T found :',T)

	if T < 2 :
		print('ERROR : Parameter T too low, needs to be >= 2')
		exit()

	### Load the batch log
	for line in f :
		## build data structure
		try:
			event = json.loads(line)
		except :
			print('LINE ERROR, JSON not recognized:',line)
			continue		
		update_data(event)

end = time.time()
print('End BATCH LOG processing: %.2f' % (end - start))	


#######################################
# D.3  Load STREAM LOGS json, update data structure and flag anomalities

start = time.time()

output = ''

with open(stream_log_file,'r') as f :
	for line in f :

		## update data structure
		try:
			event = json.loads(line)
		except :
			print('LINE ERROR, JSON not recognized',line)
			continue
		event_type = update_data(event)
		if event_type == 'P' :
			flagged_event = flag_purchase(event)
			if flagged_event :

				### re ordering the output to pass the diff test 
				output += json.dumps(OrderedDict([\
					("event_type" , flagged_event['event_type']),\
					("timestamp" , flagged_event['timestamp']),\
					("id" , flagged_event['id']),\
					("amount" ,flagged_event['amount']),\
					("mean" ,flagged_event['mean']),\
					("sd" , flagged_event['sd'])\
					]))  +'\n'

end = time.time()
print('End STREAM LOG processing: %.2f' % (end - start))	

#print(social_data)
#print(purchase_data)

###########################################
# D.4  Display Indicators and Save Output

print('time_social: %.2f' %time_social)
print('time_transaction: %.2f' %time_transaction )
print('time_compute: %.2f' %time_compute )

with open(output_file,'w') as f:
	f.write(output)
