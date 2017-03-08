import csv
import sys
import numpy as np
from tabulate import tabulate
from termcolor import colored

data = {"table1" : None, "table2" : None, "table3" : None}
headers = {"table1" : [], "table2" : [], "table3" : []}
func_list = ['max', 'min', 'avg', 'sum']


# Since we'll deal with integers only therefore mapped to int
with open("../table1.csv") as f1:
	data["table1"] = np.array([[int(j) for j in i] for i in list(csv.reader(f1.readlines()))])
	f1.close()

with open("../table2.csv") as f2:
	data["table2"] = np.array([[int(j) for j in i] for i in list(csv.reader(f2.readlines()))])
	f2.close()

with open("../table3.csv") as f3:
	data["table3"] = np.array([[int(j) for j in i] for i in list(csv.reader(f3.readlines()))])
	f3.close()

def get_header():
	skeleton = open("../metadata.txt").readlines()
	start_ind = "<begin_table>"
	end_ind = "<end_table>"
	
	switch = False
	table_name = None

	index = 0
	while index < len(skeleton):
		if not switch:
			if skeleton[index].rstrip() == start_ind:
				switch = True
				index += 1
				table_name = skeleton[index].rstrip()
		else:
			value = skeleton[index].rstrip()
			while value != end_ind:
				headers[table_name].append(value)
				index += 1
				value = skeleton[index].rstrip()
			switch = False
		index += 1

def print_table(pdata, pheaders):
	print(tabulate(pdata, pheaders))

def raise_error(message):
	print(colored("Query Error: " + message, "red"))
	sys.exit(0)

def query_parser(query):
	# All syntactic errors will be caught here
	distinct = False
	cond = ""

	if(query.find("select ") != -1):
		# Trim out the first select we get and keep the latter part for further analysis
		query = query.split("select", 1)[-1]
	else:
		raise_error("Select clause necessary")
	
	if (query.find(" from ") != -1):
		# Similar to "select" we'll split from the first from
		query = query.split("from", 1)
	else:
		raise_error("From clause necessary")
	
	col_info = query[0].split(" distinct ", 1)
	if(len(col_info) == 2):
		distinct = True
		# Nothing should be there between distinct and select
		if(len(col_info[0].strip()) > 0):
			raise_error("Found an element between select distinct")
		if(len(col_info[1].strip()) == 0):
			raise_error("Column name not provided")
		col_info = ("").join(col_info[1].split()).split(",")
	else:
		if(len(col_info[0].strip()) == 0):
			raise_error("Column name not provided")
		col_info = ("").join(col_info[0].split()).split(",")



	query[1] = query[1].split(" where ", 1)
	db_info = ("").join(query[1][0].split()).split(",")
	if len(query[1]) > 1:
		cond = query[1][1].strip()
		if(len(cond) == 0):
			raise_error("No condition placed after where clause")
	if(len(query[1][0].strip()) == 0):
		raise_error("No table name provided")
	return (col_info, distinct, db_info, cond)

def col_generate(ver_col, repeat_size, total_size, func_col):
	# Generates column's for the bigger joint table

	new_col = []
	if func_col != None:
		if func_col == 0:
			new_col.append(max(ver_col))
		elif func_col == 1:
			new_col.append(min(ver_col))
		elif func_col == 2:
			try:
				new_col.append(sum(ver_col, 0.0)/len(ver_col))
			except ZeroDivisionError:
				raise_error("Trying to average an empty column")
		else:
			new_col.append(sum(ver_col))
		ver_col = new_col

	col = []
	while total_size != 0:
		for value in ver_col:
			loop_size = repeat_size
			while loop_size != 0:
				col.append(value)
				loop_size -= 1
			total_size -= repeat_size
	return col


def where_itptr(cond):
	# joiners and & or
	# equalities > <  = >= <= 
	# TODO Check correctness of column names
	pass
	# print(cond)


def distinct_row(rows):
	# Takes in 2D array and removes any duplicate rows
	out_row = []
	for row in rows:
		row = list(row)
		if row not in out_row:
			out_row.append(row)
	return out_row


def validate_col(query_cols, query_tables):
	# Possible to replace this whole by storing an array of valid column names
	for col_name in query_cols:
		
		func_flag = False
		# Parses the function from the column name
		index = query_cols.index(col_name)
		
		temp = col_name.split('(')
		if len(temp) > 1:
			func_flag = True
			col_name = temp[1].split(')')[0]
			if (len(col_name) == len(temp[1])):
				raise_error("Closing bracket required")
			if temp[0] not in func_list:
				raise_error("Function " + temp[0] + " not recognized")
 
		if col_name != '*':
			flag = False
			for query_table in query_tables:
				if col_name in headers[query_table]:
					if flag:
						raise_error("Ambiguous column name " + col_name)
					flag = True
			
			if not flag:
				# column names can also be like table1.col1 etc.
				for query_table in query_tables:
					parse_pattern = query_table + "."
					if ''.join(col_name.split(parse_pattern)) in headers[query_table]:
						flag = True
					if ''.join(col_name.split(parse_pattern)) == '*':
						if func_flag:
							raise_error("Wildcard not allowed inside function")
						else:
							flag = True
				
				if not flag:
					if (len(query_cols) == 1 and col_name[0] == "*"):
						raise_error("Only wildcard (*) is allowed by the system")
					else:
						raise_error("No column with the name " + col_name)
		elif func_flag:
			raise_error("Wildcard not allowed inside function")




def query_itptr(query):
	# All the semantics are checked here

	query_cols = query[0] 
	query_tables = query[2]

	# Checking if no incorrect table enteries are there
	for table_name in query_tables:
		if table_name not in data:
			raise_error("No table with the name " + table_name + " exists")

	validate_col(query_cols, query_tables)


	base_table_size = {}
	base_table_rank = []
	base_table_perm = {}
	# Contains which column belongs to which table
	base_query_tabl = [None] * len(query_cols)

	for query_table in query_tables:
		base_table_size[query_table] = 0

	total_cols = 1
	for col in query_cols:
		index = query_cols.index(col)
		for query_table in query_tables:
			rank_flag = 0
			query_col = col
			query_col = ''.join(query_col.split(query_table + "."))
			if query_col == '*':
				base_table_size[query_table] = len(data[query_table])
				base_query_tabl[index] = query_table
				rank_flag = 1
			elif query_col in headers[query_table]:
				base_table_size[query_table] = len(data[query_table])
				base_query_tabl[index] = query_table
				rank_flag = 2
			elif len(query_col.split("(")) > 1:
				if query_col.split("(")[1].split(")")[0] in headers[query_table]:
					base_table_size[query_table] = max(1, base_table_size[query_table])
					base_query_tabl[index] = query_table
					rank_flag = 2
			if query_table not in base_table_rank and rank_flag > 0:
				base_table_rank.append(query_table)
			if rank_flag > 1:
				break

	for query_table in query_tables:
		if base_table_size[query_table] == 0:
			base_table_size[query_table] = len(data[query_table])
		total_cols *= base_table_size[query_table]
		if query_table not in base_table_rank:
			base_table_rank.append(query_table)

	curr_total_cols = total_cols
	for table_name in base_table_rank:
		curr_total_cols /= base_table_size[table_name]
		base_table_perm[table_name] = int(curr_total_cols)

	qdata = []
	qheader = []


	for col in query_cols:
		query_col = col
		index = query_cols.index(query_col)
		func_col = None
		temp = query_col.split('(')
		if len(temp) > 1:
			func_col = func_list.index(temp[0])
			query_col = ''.join(temp[1].split(')'))
		query_col = ''.join(query_col.split(base_query_tabl[index] + "."))
		
		if col == "*":
			for query_table in query_tables:
				for header in headers[query_table]:
					qheader.append(header)
					qdata.append(col_generate(data[query_table][:, headers[query_table].index(header)], base_table_perm[query_table],total_cols, func_col))
		else:
			query_table = base_query_tabl[index]
			if query_col == "*":
				for header in headers[query_table]:
					qheader.append(header)
					qdata.append(col_generate(data[query_table][:, headers[query_table].index(header)], base_table_perm[query_table],total_cols, func_col))
			else:
				header = query_col
				qheader.append(header)
				qdata.append(col_generate(data[query_table][:, headers[query_table].index(header)], base_table_perm[query_table],total_cols, func_col))

	# Apply where clause filters

	where_itptr(query[3])


	qdata = np.transpose(qdata)
	if query[1]:
		qdata = distinct_row(qdata)

	print_table(qdata, qheader)


def query_direct(query):
	query = query_parser(query)
	# First value denotes which to search
	# Second value denotes if distinct or not
	# Third value denotes which table to search from
	# Fourth value has where condition
	query_itptr(query)


if __name__ == '__main__':
	get_header()
	query_direct(sys.argv[1])
