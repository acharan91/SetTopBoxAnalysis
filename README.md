# SetTopBoxAnalysis
Set Top Box Analysis using Spark Scala


KPIs 
1. Filter all the record with event_id=100 
  i. Get the top five devices with maximum duration 
  ii. Get the top five Channels with maximum duration 
  iii. Total number of devices with ChannelType="LiveTVMediaChannel" 
2. Filter all the record with event_id=101 
  i. Get the total number of devices with PowerState="On/Off" 
3. Filter all the record with Event 102/113 
  i. Get the maximum price group by offer_id 
4. Filter all the record with event_id=118 
  i. Get the min and maximum duration 
5. Filter all the record with Event 0 
  i. Calculate how many junk records are thier having BadBlocks in xml column 
6. Filter all the record with Event 107 
  i. group all the ButtonName with thier device_ids 
7. Filter all the record with Event 115/118 
  i. Get the  duration group by program_id 
  ii. Total number of devices with frequency="Once" 
