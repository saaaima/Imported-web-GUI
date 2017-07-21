# Imported-web-GUI
This is a platform for basic data manipulation and cleaning. It is supposed to find all the missing airings from TVA dashboard compared with original postlog file. 
It serves as an interactive GUI that helps Chirs, Birte and I prepare cleaned data for any further analysis. 
Current functions of this platform includes:

1) read tva data from API

 ![Sceenshot](https://github.com/saaaima/Imported-web-GUI/blob/master/login%20page.png)
 
2) switch local time to utc time, and provide epoch timestamp.

3) display possible reminder spots and drop duplications;
  ![Sceenshot](https://github.com/saaaima/Imported-web-GUI/blob/master/reminder_spots.png)






4) channel name matching; 

  a) Users are give choices to match common channels from postlog to tva.
  
  ![Sceenshot](https://github.com/saaaima/Imported-web-GUI/blob/master/channel_change.png)
  
  
   
 
  
  
  
  b) Channels from postlog but not in tva are matched to API automatically. Results are displayed to help users understand
     which channel name has been changed. Users can also correct results if they are mismatched.
     
  c) all the changes will be stored into a database, therefore there will not be any repeateable works to do in the future.
  
   ![Sceenshot](https://github.com/saaaima/Imported-web-GUI/blob/master/channel%20database.png)

5) commercial name matching;

  a) User are give choices to match commercial names from matched-postlog to tva, with the help of the commercial distribution
     over channels. 
  ![Sceenshot](https://github.com/saaaima/Imported-web-GUI/blob/master/commercial_change.png)   
  
  
   b) Uses are given choices to decide if they want to save the changing of commercials into database. 
  
  
  c) All the agreed changes will be stored into a database, and no more repeatable works to do in the future. 
  ![Sceenshot](https://github.com/saaaima/Imported-web-GUI/blob/master/commercial%20database.png)

6) airings matching. Results include missing airings, free airings, missing channels and free channels;

  a) for ambiguous airings, users are given choices to pick right airings with the aid of commercial names. 

7) create an import file with information customer_id,channel_id, commercial_id,broadcast_start_timestamp for all the missing airings. 
