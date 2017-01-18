nohup python3 feeder.py -db bdc -u admin -pw _admin_ -doc $1 -m 0 -q 5 -f $2 -l $3 > 0_feed.out 2> 0_feed.err &
nohup python3 feeder.py -db bdc -u admin -pw _admin_ -doc $1 -m 1 -q 5 -f $2 -l $3 > 1_feed.out 2> 1_feed.err &
nohup python3 feeder.py -db bdc -u admin -pw _admin_ -doc $1 -m 2 -q 5 -f $2 -l $3 > 2_feed.out 2> 2_feed.err &
nohup python3 feeder.py -db bdc -u admin -pw _admin_ -doc $1 -m 3 -q 5 -f $2 -l $3 > 3_feed.out 2> 3_feed.err &
nohup python3 feeder.py -db bdc -u admin -pw _admin_ -doc $1 -m 4 -q 5 -f $2 -l $3 > 4_feed.out 2> 4_feed.err &
