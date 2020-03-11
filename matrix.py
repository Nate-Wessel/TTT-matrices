# computes travel times among a set of points over some time range

# run with ...
# java -Xmx4G -cp otp.jar:jython.jar org.opentripplanner.standalone.OTPMain --script matrix.py

from org.opentripplanner.scripting.api import OtpsEntryPoint
from datetime import datetime, timedelta
from time import sleep
from time import time as unix_time
import csv, threading, os

router_name = 'mbta-17480'
ODfile  = '/home/nate/Dropbox/diss/analysis/ODsets/mbta.csv'

out_dir = '/home/nate/Dropbox/'+router_name+'/'

# define the start time
start_time = datetime( year=2017, month=11, day=10, hour=6, minute=0 )
# and go for how long?
end_time = start_time + timedelta(hours=4,minutes=1)
# how much time to traverse?
print 'going from ', start_time, 'until', end_time

# ask how many processes to use
max_threads = int(raw_input('how many threads? -> '))

# read in the O/D points to route between 
points = []
with open(ODfile, 'rb') as point_csv:
	point_reader = csv.DictReader(point_csv)
	for record in point_reader:
		points.append({
			'id'  : str(record['uid']),
			'lat' : float(record['lat']),
			'lon' : float(record['lon'])
		})
# load the graph
otp = OtpsEntryPoint.fromArgs( [ 
	'--graphs', 'data/graphs', 
	'--router', router_name 
] )


def process_matrix(time):
	# check whether this file has actually already been written
	output_file = out_dir+str(time)+'.csv'
	# and if it has skip it. No need to do that again
	if os.path.exists(output_file): 
		print output_file,'already exists'
		return
	start = unix_time()
	# make a router
	router = otp.getRouter()
	request = otp.createRequest()
	request.setModes('WALK, TRANSIT')
	request.setMaxTimeSec(1*3600) # seconds
	request.setClampInitialWait(0) 
	request.setMaxWalkDistance(3000) # meters?
	request.setDateTime(
		time.year, time.month, time.day, 
		time.hour, time.minute, time.second
	)
	# open the output list with list of header values
	out = [ ['-']+[ p['id'] for p in points ] ]
	# loop over origins
	for o in points:
		if int(o['id']) % 10 == 0:
			print router_name, str(time), o['id']+'/'+str(len(points))
		# add origin to request and walk out from that point
		request.setOrigin( o['lat'], o['lon'] )
		spt = router.plan( request )
		if spt is None:
			# add an empty row if the origin doesn't reach anything
			out.append( [o['id']]+['-']*len(points) )
			continue
		# loop over destinations, getting times
		times = []
		for d in points:
			result = spt.eval( d['lat'], d['lon'] )
			if result is not None:
				times.append( str(result.getTime()) )
			else:
				times.append('-')
		# add the times for this origin
		out.append( [o['id']] + times )
	# format and write the output. Currently a list of lists of strings
	with open(output_file,'w+') as out_file:
		# add commas and newlines and write to file
		formatted_text = '\n'.join( [ ','.join(r) for r in out ] )
		out_file.write(formatted_text)
	print unix_time() - start,'seconds to complete'

# generate list of times to process, minute by minute
time = start_time
while time < end_time:
	# one thread is used for sleeping in this loop
	if threading.active_count() <= max_threads:
		t = threading.Thread( target=process_matrix, args=(time,) )
		t.start()
		time += timedelta( minutes = 1 )
	else:
		sleep(1)



